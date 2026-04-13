"""AetherGrid v2.0 - gRPC Service Definitions.

Defines the gRPC services for:
1. TaskService - Client API for task submission and management
2. RaftService - Internal Raft protocol communication
3. WorkerService - Worker registration and task streaming
"""

import grpc
from typing import AsyncIterator, Iterator
from dataclasses import dataclass
import asyncio
from concurrent import futures
import json


# ============== Service Implementation (without protobuf) ==============
# Using simple dict-based messages for simplicity
# In production, use generated protobuf code


class TaskServiceServicer:
    """gRPC service for client-facing Task API."""
    
    def __init__(self, cluster: 'ClusterInterface'):
        self.cluster = cluster
    
    async def SubmitTask(
        self,
        request: dict,
        context: grpc.ServicerContext,
    ) -> dict:
        """Submit a new task."""
        # Extract auth info
        user = request.get("auth", {}).get("user", "anonymous")
        
        # Submit to cluster
        result = await self.cluster.submit_task(
            name=request.get("name", "unnamed"),
            namespace=request.get("namespace", "default"),
            image=request.get("image", ""),
            args=request.get("args", []),
            env=request.get("env", []),
            resources=request.get("resources", {}),
            labels=request.get("labels", {}),
            submitted_by=user,
        )
        
        return {"success": result.success, "task": result.task.to_dict() if result.task else None}
    
    async def GetTask(
        self,
        request: dict,
        context: grpc.ServicerContext,
    ) -> dict:
        """Get task by ID."""
        task_id = request.get("task_id", {})
        task = await self.cluster.get_task(task_id)
        
        if task is None:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Task not found",
            )
        
        return {"task": task.to_dict() if task else None}
    
    async def ListTasks(
        self,
        request: dict,
        context: grpc.ServicerContext,
    ) -> dict:
        """List tasks with filtering."""
        tasks = await self.cluster.list_tasks(
            namespace=request.get("namespace"),
            status=request.get("status"),
            limit=request.get("limit", 100),
        )
        
        return {
            "tasks": [t.to_dict() for t in tasks],
            "next_page_token": "",
        }
    
    async def WatchTasks(
        self,
        request: dict,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[dict]:
        """
        Stream task updates in real-time.
        
        Clients can watch for:
        - Task status changes
        - Task completion
        - Task failure
        """
        namespace = request.get("namespace")
        task_id = request.get("task_id")
        
        async for event in self.cluster.watch_tasks(namespace=namespace, task_id=task_id):
            yield event


class RaftServiceServicer:
    """gRPC service for internal Raft communication."""
    
    def __init__(self, node: 'RaftNodeInterface'):
        self.node = node
    
    async def AppendEntries(
        self,
        request: dict,
        context: grpc.ServicerContext,
    ) -> dict:
        """Handle AppendEntries RPC."""
        # Put message in node's inbox
        await self.node.receive_message(request.get("leader_id", 0), request)
        
        # Wait for response
        response = await self.node.get_response()
        return response
    
    async def RequestVote(
        self,
        request: dict,
        context: grpc.ServicerContext,
    ) -> dict:
        """Handle RequestVote RPC."""
        await self.node.receive_message(request.get("candidate_id", 0), request)
        response = await self.node.get_response()
        return response
    
    async def InstallSnapshot(
        self,
        request_iterator: AsyncIterator[dict],
        context: grpc.ServicerContext,
    ) -> dict:
        """Handle streaming InstallSnapshot RPC."""
        chunks = []
        request = None
        
        async for req in request_iterator:
            request = req
            chunks.append(req.get("data", b""))
            
            if req.get("done", False):
                break
        
        if request:
            request["data"] = b''.join(chunks)
            await self.node.receive_message(request.get("leader_id", 0), request)
            response = await self.node.get_response()
            return response
        
        return {"term": 0, "last_included_index": 0}


class WorkerServiceServicer:
    """gRPC service for worker nodes."""
    
    def __init__(self, cluster: 'ClusterInterface'):
        self.cluster = cluster
    
    async def RegisterWorker(
        self,
        request: dict,
        context: grpc.ServicerContext,
    ) -> dict:
        """Register a new worker node."""
        result = await self.cluster.register_node(
            node_id=request.get("node_id"),
            hostname=request.get("hostname"),
            capacity=request.get("capacity", {}),
            labels=request.get("labels", {}),
        )
        
        return {"success": result.success, "error": result.error}
    
    async def StreamAssignments(
        self,
        request: dict,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[dict]:
        """
        Stream task assignments to workers.
        
        This is the key method for real-time task distribution.
        Workers connect and receive task assignments as they are scheduled.
        
        Each assignment includes:
        - Task definition
        - Fencing token (for zombie worker protection)
        - Lease expiry index (for worker safety)
        """
        node_id = request.get("node_id")
        
        async for assignment in self.cluster.watch_assignments(node_id):
            yield assignment
    
    async def ReportStatus(
        self,
        request: dict,
        context: grpc.ServicerContext,
    ) -> dict:
        """
        Receive status reports from workers.
        
        Workers report task completion/failure with fencing tokens.
        Stale tokens are rejected to prevent zombie workers.
        """
        result = await self.cluster.report_task_status(
            task_id=request.get("task_id"),
            node_id=request.get("node_id"),
            fencing_token=request.get("fencing_token"),
            status=request.get("status"),
            exit_code=request.get("exit_code", 0),
            error_message=request.get("error_message", ""),
        )
        
        return {
            "accepted": result.success,
            "rejection_reason": result.error or "",
            "current_fencing_token": result.fencing_token or 0,
        }
    
    async def Heartbeat(
        self,
        request: dict,
        context: grpc.ServicerContext,
    ) -> dict:
        """Receive heartbeat from worker with current log index."""
        await self.cluster.update_node_heartbeat(
            node_id=request.get("node_id"),
            running_tasks=request.get("running_tasks", []),
            current_log_index=request.get("current_log_index", 0),
        )
        
        # Return current cluster log index for worker lease tracking
        return {
            "cluster_log_index": await self.cluster.get_log_index(),
        }


# ============== Server Setup ==============

async def start_grpc_server(
    cluster: 'ClusterInterface',
    node: 'RaftNodeInterface',
    port: int = 50051,
    use_tls: bool = False,
    cert_file: str = None,
    key_file: str = None,
    ca_file: str = None,
) -> grpc.aio.Server:
    """Start the gRPC server with all services."""
    
    server = grpc.aio.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_receive_message_length', 64 * 1024 * 1024),  # 64MB
            ('grpc.max_send_message_length', 64 * 1024 * 1024),
        ],
    )
    
    # Register services (using generic handlers since we're not using generated protobuf)
    # In production, use: add_TaskServiceServicer_to_server, etc.
    
    # For now, we'll use a simple JSON-based protocol
    from .json_grpc import JsonTaskService, JsonRaftService, JsonWorkerService
    
    server.add_generic_rpc_handlers([
        JsonTaskService(cluster),
        JsonRaftService(node),
        JsonWorkerService(cluster),
    ])
    
    # Configure security
    if use_tls:
        with open(cert_file, 'rb') as f:
            cert = f.read()
        with open(key_file, 'rb') as f:
            key = f.read()
        with open(ca_file, 'rb') as f:
            ca = f.read()
        
        server_credentials = grpc.ssl_server_credentials(
            [(key, cert)],
            root_certificates=ca,
            require_client_auth=True,
        )
        server.add_secure_port(f'[::]:{port}', server_credentials)
    else:
        server.add_insecure_port(f'[::]:{port}')
    
    await server.start()
    return server
