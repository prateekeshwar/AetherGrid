"""AetherGrid gRPC Services Implementation.

Implements three gRPC services:
1. RaftService - Internal Raft consensus (AppendEntries, RequestVote, InstallSnapshot)
2. TaskService - Client API for task submission and queries
3. WorkerService - Worker-to-leader communication

Usage:
    # Start a gRPC server
    server = create_grpc_server(raft_node, state_machine, port=50051)
    await server.start()
"""

import asyncio
import grpc
from concurrent import futures
from typing import Optional, Callable, Any, Dict

from .generated import aethergrid_pb2
from .generated import aethergrid_pb2_grpc
from .core import ProcessTableState, TaskStatus, TaskID, NodeID


class RaftServiceServicer(aethergrid_pb2_grpc.RaftServiceServicer):
    """Implementation of Raft consensus RPCs."""
    
    def __init__(self, raft_node):
        self.raft_node = raft_node
    
    async def AppendEntries(
        self,
        request: aethergrid_pb2.AppendEntriesRequest,
        context: grpc.aio.ServicerContext,
    ) -> aethergrid_pb2.AppendEntriesResponse:
        """Handle AppendEntries RPC from leader."""
        # Convert proto entries to dict format for RaftNode
        entries = []
        for entry in request.entries:
            command = self._parse_command(entry)
            entries.append({
                "term": entry.term,
                "index": entry.index,
                "command": command,
            })
        
        msg = {
            "type": "AppendEntries",
            "term": request.term,
            "leader_id": request.leader_id,
            "prev_log_index": request.prev_log_index,
            "prev_log_term": request.prev_log_term,
            "entries": entries,
            "leader_commit": request.leader_commit,
        }
        
        response = await self.raft_node.handle_append_entries(msg)
        
        return aethergrid_pb2.AppendEntriesResponse(
            term=response.get("term", 0),
            success=response.get("success", False),
            match_index=response.get("match_index", 0),
            conflict_index=response.get("conflict_index", 0),
        )
    
    async def RequestVote(
        self,
        request: aethergrid_pb2.RequestVoteRequest,
        context: grpc.aio.ServicerContext,
    ) -> aethergrid_pb2.RequestVoteResponse:
        """Handle RequestVote RPC from candidate."""
        msg = {
            "type": "RequestVote",
            "term": request.term,
            "candidate_id": request.candidate_id,
            "last_log_index": request.last_log_index,
            "last_log_term": request.last_log_term,
        }
        
        response = await self.raft_node.handle_request_vote(msg)
        
        return aethergrid_pb2.RequestVoteResponse(
            term=response.get("term", 0),
            vote_granted=response.get("vote_granted", False),
        )
    
    async def InstallSnapshot(
        self,
        request: aethergrid_pb2.InstallSnapshotRequest,
        context: grpc.aio.ServicerContext,
    ) -> aethergrid_pb2.InstallSnapshotResponse:
        """Handle InstallSnapshot RPC from leader."""
        msg = {
            "type": "InstallSnapshot",
            "term": request.term,
            "leader_id": request.leader_id,
            "last_included_index": request.last_included_index,
            "last_included_term": request.last_included_term,
            "data": request.snapshot_data,
        }
        
        response = await self.raft_node.handle_install_snapshot(msg)
        
        return aethergrid_pb2.InstallSnapshotResponse(
            term=response.get("term", 0),
            last_included_index=response.get("last_included_index", 0),
        )
    
    def _parse_command(self, entry) -> dict:
        """Parse command from log entry."""
        if not entry.HasField('command'):
            return {}
        
        cmd = entry.command
        
        if cmd.HasField('submit_task'):
            sc = cmd.submit_task
            return {
                "type": "submit_task",
                "name": sc.name,
                "namespace": sc.namespace,
                "image": sc.image,
                "args": list(sc.args),
                "env": list(sc.env),
            }
        elif cmd.HasField('schedule_task'):
            st = cmd.schedule_task
            return {
                "type": "schedule_task",
                "task_id": {"shard": st.task_id.shard, "sequence": st.task_id.sequence},
                "node_id": {"id": st.node_id.id},
                "fencing_token": st.fencing_token,
            }
        elif cmd.HasField('complete_task'):
            ct = cmd.complete_task
            return {
                "type": "complete_task",
                "task_id": {"shard": ct.task_id.shard, "sequence": ct.task_id.sequence},
                "node_id": {"id": ct.node_id.id},
                "fencing_token": ct.fencing_token,
                "exit_code": ct.exit_code,
            }
        elif cmd.HasField('fail_task'):
            ft = cmd.fail_task
            return {
                "type": "fail_task",
                "task_id": {"shard": ft.task_id.shard, "sequence": ft.task_id.sequence},
                "node_id": {"id": ft.node_id.id},
                "fencing_token": ft.fencing_token,
                "error_message": ft.error_message,
            }
        
        return {}


class TaskServiceServicer(aethergrid_pb2_grpc.TaskServiceServicer):
    """Implementation of client-facing Task API."""
    
    def __init__(self, raft_node, state_machine: ProcessTableState, logs_store: Callable, task_dispatcher: Callable = None):
        self.raft_node = raft_node
        self.state_machine = state_machine
        self.logs_store = logs_store
        self.task_dispatcher = task_dispatcher
    
    async def SubmitTask(
        self,
        request: aethergrid_pb2.SubmitTaskRequest,
        context: grpc.aio.ServicerContext,
    ) -> aethergrid_pb2.SubmitTaskResponse:
        """Submit a new task via gRPC."""
        # Check if we're the leader
        if not self._is_leader():
            return aethergrid_pb2.SubmitTaskResponse(
                success=False,
                error="Not the leader - please redirect to leader node",
            )
        
        # Build command
        command = {
            "type": "submit_task",
            "name": request.name,
            "namespace": request.namespace or "default",
            "image": request.image,
            "args": list(request.args),
            "env": list(request.env),
            "labels": dict(request.labels),
            "priority": request.priority,
        }
        
        # Submit to Raft
        success, result = await self.raft_node.submit_command(command)
        
        if not success:
            return aethergrid_pb2.SubmitTaskResponse(
                success=False,
                error=str(result),
            )
        
        # Get task from result
        task = result.task if hasattr(result, 'task') else None
        if task:
            # Dispatch to worker if dispatcher is available
            if self.task_dispatcher:
                await self.task_dispatcher(task)
            
            return aethergrid_pb2.SubmitTaskResponse(
                success=True,
                task=self._task_to_proto(task),
            )
        
        return aethergrid_pb2.SubmitTaskResponse(success=True)
    
    async def GetTask(
        self,
        request: aethergrid_pb2.GetTaskRequest,
        context: grpc.aio.ServicerContext,
    ) -> aethergrid_pb2.GetTaskResponse:
        """Get task by ID."""
        task_id = self._parse_task_id(request.task_id)
        if not task_id:
            return aethergrid_pb2.GetTaskResponse(found=False)
        
        task = self.state_machine.tasks.get(task_id)
        if not task:
            return aethergrid_pb2.GetTaskResponse(found=False)
        
        return aethergrid_pb2.GetTaskResponse(
            found=True,
            task=self._task_to_proto(task),
        )
    
    async def ListTasks(
        self,
        request: aethergrid_pb2.ListTasksRequest,
        context: grpc.aio.ServicerContext,
    ) -> aethergrid_pb2.ListTasksResponse:
        """List tasks with optional filtering."""
        tasks = []
        
        for task in self.state_machine.tasks.values():
            # Apply namespace filter
            if request.namespace and task.namespace != request.namespace:
                continue
            
            # Apply status filter
            if request.status_filter:
                try:
                    status = TaskStatus(request.status_filter.lower())
                    if task.status != status:
                        continue
                except ValueError:
                    pass
            
            tasks.append(self._task_to_proto(task))
            
            if request.limit > 0 and len(tasks) >= request.limit:
                break
        
        return aethergrid_pb2.ListTasksResponse(tasks=tasks)
    
    async def GetLogs(
        self,
        request: aethergrid_pb2.GetLogsRequest,
        context: grpc.aio.ServicerContext,
    ) -> aethergrid_pb2.GetLogsResponse:
        """Get logs for a task."""
        logs = self.logs_store(request.task_id)
        
        # Get task status
        task_id = self._parse_task_id(request.task_id)
        status = "unknown"
        if task_id:
            task = self.state_machine.tasks.get(task_id)
            if task:
                status = task.status.value
        
        return aethergrid_pb2.GetLogsResponse(
            lines=[entry.get("line", "") for entry in logs],
            status=status,
        )
    
    async def CancelTask(
        self,
        request: aethergrid_pb2.GetTaskRequest,
        context: grpc.aio.ServicerContext,
    ) -> aethergrid_pb2.GetTaskResponse:
        """Cancel a task."""
        if not self._is_leader():
            return aethergrid_pb2.GetTaskResponse(found=False)
        
        task_id = self._parse_task_id(request.task_id)
        if not task_id:
            return aethergrid_pb2.GetTaskResponse(found=False)
        
        command = {
            "type": "cancel_task",
            "task_id": task_id.to_dict(),
            "reason": "Cancelled via gRPC",
        }
        
        success, result = await self.raft_node.submit_command(command)
        
        if success and hasattr(result, 'task'):
            return aethergrid_pb2.GetTaskResponse(
                found=True,
                task=self._task_to_proto(result.task),
            )
        
        return aethergrid_pb2.GetTaskResponse(found=False)
    
    def _is_leader(self) -> bool:
        """Check if this node is the leader."""
        from .core import RaftState
        return self.raft_node.state == RaftState.LEADER
    
    def _parse_task_id(self, task_id_str: str) -> Optional[TaskID]:
        """Parse task ID from string format 'shard:sequence'."""
        try:
            parts = task_id_str.split(":")
            if len(parts) == 2:
                return TaskID(shard=int(parts[0]), sequence=int(parts[1]))
        except (ValueError, AttributeError):
            pass
        return None
    
    def _task_to_proto(self, task) -> aethergrid_pb2.Task:
        """Convert Task dataclass to proto Task."""
        status_map = {
            TaskStatus.PENDING: aethergrid_pb2.TASK_STATUS_PENDING,
            TaskStatus.SCHEDULED: aethergrid_pb2.TASK_STATUS_SCHEDULED,
            TaskStatus.RUNNING: aethergrid_pb2.TASK_STATUS_RUNNING,
            TaskStatus.SUCCEEDED: aethergrid_pb2.TASK_STATUS_SUCCEEDED,
            TaskStatus.FAILED: aethergrid_pb2.TASK_STATUS_FAILED,
            TaskStatus.CANCELLED: aethergrid_pb2.TASK_STATUS_CANCELLED,
            TaskStatus.TIMEOUT: aethergrid_pb2.TASK_STATUS_TIMEOUT,
            TaskStatus.ORPHANED: aethergrid_pb2.TASK_STATUS_ORPHANED,
        }
        
        proto_task = aethergrid_pb2.Task(
            id=aethergrid_pb2.TaskID(
                shard=task.id.shard,
                sequence=task.id.sequence,
            ),
            name=task.name,
            namespace=task.namespace,
            image=task.image,
            args=task.args,
            env=task.env,
            status=status_map.get(task.status, aethergrid_pb2.TASK_STATUS_UNSPECIFIED),
            exit_code=task.exit_code,
            error_message=task.error_message,
            fencing_token=task.fencing_token,
            labels=task.labels,
            priority=task.priority,
            checkpoint_url=task.checkpoint_url,
            checkpoint_index=task.checkpoint_index,
            checkpoint_log_index=task.checkpoint_log_index,
        )
        
        if task.assigned_node:
            proto_task.assigned_node.id = task.assigned_node.id
        
        return proto_task


class WorkerServiceServicer(aethergrid_pb2_grpc.WorkerServiceServicer):
    """Implementation of worker-to-leader communication."""
    
    def __init__(self, raft_node, state_machine: ProcessTableState, task_dispatcher: Callable = None):
        self.raft_node = raft_node
        self.state_machine = state_machine
        self.task_dispatcher = task_dispatcher
    
    async def RegisterNode(
        self,
        request: aethergrid_pb2.RegisterNodeRequest,
        context: grpc.aio.ServicerContext,
    ) -> aethergrid_pb2.RegisterNodeResponse:
        """Register a new worker node."""
        if not self._is_leader():
            return aethergrid_pb2.RegisterNodeResponse(accepted=False)
        
        command = {
            "type": "register_node",
            "node_id": {"id": request.node_id},
            "hostname": request.hostname,
            "capacity": {
                "cpu_millis": request.capacity.cpu_millis,
                "memory_bytes": request.capacity.memory_bytes,
                "gpu_count": request.capacity.gpu_count,
            },
            "labels": dict(request.labels),
            "runtimes": list(request.runtimes),
        }
        
        success, _ = await self.raft_node.submit_command(command)
        
        return aethergrid_pb2.RegisterNodeResponse(
            accepted=success,
            leader_id=str(self.raft_node.node_id),
        )
    
    async def ReportStatus(
        self,
        request: aethergrid_pb2.TaskStatusReport,
        context: grpc.aio.ServicerContext,
    ) -> aethergrid_pb2.TaskStatusReportResponse:
        """Handle task status report from worker."""
        if not self._is_leader():
            return aethergrid_pb2.TaskStatusReportResponse(
                accepted=False,
                rejection_reason="Not the leader",
            )
        
        task_id = TaskID(
            shard=request.task_id.shard,
            sequence=request.task_id.sequence,
        )
        
        # Map proto status to internal status
        status_map = {
            aethergrid_pb2.TASK_STATUS_RUNNING: TaskStatus.RUNNING,
            aethergrid_pb2.TASK_STATUS_SUCCEEDED: TaskStatus.SUCCEEDED,
            aethergrid_pb2.TASK_STATUS_FAILED: TaskStatus.FAILED,
        }
        
        status = status_map.get(request.status, TaskStatus.RUNNING)
        
        # Build appropriate command based on status
        if status == TaskStatus.SUCCEEDED:
            command = {
                "type": "complete_task",
                "task_id": task_id.to_dict(),
                "node_id": {"id": "worker"},
                "fencing_token": request.fencing_token,
                "exit_code": request.exit_code,
            }
        elif status == TaskStatus.FAILED:
            command = {
                "type": "fail_task",
                "task_id": task_id.to_dict(),
                "node_id": {"id": "worker"},
                "fencing_token": request.fencing_token,
                "error_message": request.error_message,
            }
        else:
            command = {
                "type": "start_task",
                "task_id": task_id.to_dict(),
                "node_id": {"id": "worker"},
                "fencing_token": request.fencing_token,
            }
        
        success, result = await self.raft_node.submit_command(command)
        
        if not success:
            return aethergrid_pb2.TaskStatusReportResponse(
                accepted=False,
                rejection_reason=str(result),
            )
        
        return aethergrid_pb2.TaskStatusReportResponse(
            accepted=True,
            current_fencing_token=request.fencing_token,
        )
    
    async def Heartbeat(
        self,
        request: aethergrid_pb2.HeartbeatRequest,
        context: grpc.aio.ServicerContext,
    ) -> aethergrid_pb2.HeartbeatResponse:
        """Handle worker heartbeat."""
        return aethergrid_pb2.HeartbeatResponse(acknowledged=True)
    
    async def ReportCheckpoint(
        self,
        request: aethergrid_pb2.CheckpointReport,
        context: grpc.aio.ServicerContext,
    ) -> aethergrid_pb2.CheckpointReportResponse:
        """Handle checkpoint report from worker for long-running AI jobs."""
        if not self._is_leader():
            return aethergrid_pb2.CheckpointReportResponse(
                accepted=False,
                rejection_reason="Not the leader",
            )
        
        # Build checkpoint command
        command = {
            "type": "checkpoint_task",
            "task_id": {
                "shard": request.task_id.shard,
                "sequence": request.task_id.sequence,
            },
            "node_id": {"id": context.peer()},
            "fencing_token": request.fencing_token,
            "checkpoint_url": request.checkpoint_url,
            "checkpoint_index": request.checkpoint_index,
            "metadata": dict(request.metadata),
        }
        
        success, result = await self.raft_node.submit_command(command)
        
        if not success:
            return aethergrid_pb2.CheckpointReportResponse(
                accepted=False,
                rejection_reason=str(result),
            )
        
        return aethergrid_pb2.CheckpointReportResponse(
            accepted=True,
            current_log_index=self.state_machine.current_log_index,
        )
    
    def _is_leader(self) -> bool:
        """Check if this node is the leader."""
        from .core import RaftState
        return self.raft_node.state == RaftState.LEADER


def create_grpc_server(
    raft_node,
    state_machine: ProcessTableState,
    logs_store: Callable,
    task_dispatcher: Callable = None,
    port: int = 50051,
) -> grpc.aio.Server:
    """
    Create a gRPC server with all AetherGrid services.
    
    Args:
        raft_node: The Raft node instance
        state_machine: The process table state machine
        logs_store: Function to get logs for a task
        task_dispatcher: Function to dispatch task to worker
        port: Port to listen on
    
    Returns:
        Configured gRPC server (not started)
    """
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add services
    aethergrid_pb2_grpc.add_RaftServiceServicer_to_server(
        RaftServiceServicer(raft_node), server
    )
    aethergrid_pb2_grpc.add_TaskServiceServicer_to_server(
        TaskServiceServicer(raft_node, state_machine, logs_store, task_dispatcher), server
    )
    aethergrid_pb2_grpc.add_WorkerServiceServicer_to_server(
        WorkerServiceServicer(raft_node, state_machine, task_dispatcher), server
    )
    
    # Bind to port
    server.add_insecure_port(f'[::]:{port}')
    
    return server


# Client helper functions

def create_task_client(address: str = "localhost:50051") -> aethergrid_pb2_grpc.TaskServiceStub:
    """Create an async gRPC client for TaskService."""
    channel = grpc.aio.insecure_channel(address)
    return aethergrid_pb2_grpc.TaskServiceStub(channel)


async def submit_task_via_grpc(
    client: aethergrid_pb2_grpc.TaskServiceStub,
    name: str,
    image: str,
    args: list = None,
    env: list = None,
    namespace: str = "default",
    labels: dict = None,
) -> dict:
    """
    Submit a task via gRPC.
    
    Returns dict with 'success', 'error', 'task_id', 'status'.
    """
    request = aethergrid_pb2.SubmitTaskRequest(
        name=name,
        namespace=namespace,
        image=image,
        args=args or [],
        env=env or [],
        labels=labels or {},
    )
    
    try:
        response = await client.SubmitTask(request)
        
        if response.success and response.HasField('task'):
            return {
                "success": True,
                "task_id": f"{response.task.id.shard}:{response.task.id.sequence}",
                "status": aethergrid_pb2.TaskStatus.Name(response.task.status),
            }
        
        return {
            "success": False,
            "error": response.error,
        }
    except grpc.RpcError as e:
        return {
            "success": False,
            "error": f"gRPC error: {e.code()} - {e.details()}",
        }


async def get_task_via_grpc(
    client: aethergrid_pb2_grpc.TaskServiceStub,
    task_id: str,
) -> dict:
    """Get task status via gRPC."""
    request = aethergrid_pb2.GetTaskRequest(task_id=task_id)
    
    try:
        response = await client.GetTask(request)
        
        if response.found and response.HasField('task'):
            task = response.task
            return {
                "found": True,
                "id": f"{task.id.shard}:{task.id.sequence}",
                "name": task.name,
                "status": aethergrid_pb2.TaskStatus.Name(task.status),
                "exit_code": task.exit_code,
                "error_message": task.error_message,
                "image": task.image,
                "args": list(task.args),
            }
        
        return {"found": False}
    except grpc.RpcError as e:
        return {"found": False, "error": str(e)}


async def get_logs_via_grpc(
    client: aethergrid_pb2_grpc.TaskServiceStub,
    task_id: str,
) -> dict:
    """Get task logs via gRPC."""
    request = aethergrid_pb2.GetLogsRequest(task_id=task_id)
    
    try:
        response = await client.GetLogs(request)
        return {
            "lines": list(response.lines),
            "status": response.status,
        }
    except grpc.RpcError as e:
        return {"lines": [], "error": str(e)}
