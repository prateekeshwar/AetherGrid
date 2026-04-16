#!/usr/bin/env python3
"""AetherGrid v2.0 - Standalone Worker Daemon.

A standalone worker that joins an AetherGrid cluster via gRPC.

Features:
1. Joins cluster via RegisterNode gRPC call
2. Receives task assignments from leader
3. Executes Docker containers with GPU passthrough
4. Reports status with fencing tokens
5. Monitors lease expiry and kills tasks when expired
6. Tracks GPU/VRAM resources

Usage:
    # Start a worker with 2 GPUs
    aether worker start --node-id worker-1 --gpu-count 2 --gpu-memory 16384
    
    # Start a CPU-only worker
    aether worker start --node-id worker-2
"""

import asyncio
import subprocess
import os
import signal
import json
import time
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from enum import Enum

import grpc


class WorkerState(Enum):
    """Worker daemon states."""
    INITIALIZING = "initializing"
    REGISTERED = "registered"
    RUNNING = "running"
    DISCONNECTED = "disconnected"
    STOPPED = "stopped"


@dataclass
class RunningTask:
    """A currently running task."""
    task_id: str
    fencing_token: int
    lease_expiry_index: int
    process: Optional[asyncio.subprocess.Process] = None
    container_id: Optional[str] = None
    started_at: float = 0.0
    resources: Dict[str, int] = field(default_factory=dict)


class WorkerDaemon:
    """
    Standalone worker daemon with gRPC connectivity.
    
    Key features:
    1. Registers with cluster via gRPC
    2. Executes Docker containers with GPU support
    3. Tracks lease expiry and kills tasks
    4. Reports status with fencing tokens
    """
    
    HEARTBEAT_INTERVAL = 5.0
    CONNECTION_TIMEOUT = 15.0
    
    def __init__(
        self,
        node_id: str,
        cluster_address: str,
        hostname: Optional[str] = None,
        cpu_millis: int = 4000,
        memory_mb: int = 8192,
        gpu_count: int = 0,
        gpu_memory_mb: int = 0,
        labels: Optional[Dict[str, str]] = None,
        runtime: str = "docker",
    ):
        self.node_id = node_id
        self.cluster_address = cluster_address
        self.hostname = hostname or socket.gethostname()
        self.cpu_millis = cpu_millis
        self.memory_mb = memory_mb
        self.gpu_count = gpu_count
        self.gpu_memory_mb = gpu_memory_mb
        self.labels = labels or {}
        self.runtime = runtime
        
        # State
        self.state = WorkerState.INITIALIZING
        self.running_tasks: Dict[str, RunningTask] = {}
        
        # Resource tracking
        self.allocated_cpu = 0
        self.allocated_memory_mb = 0
        self.allocated_gpu_count = 0
        self.allocated_gpu_memory_mb = 0
        
        # Lease tracking
        self.cluster_log_index: int = 0
        self.last_heartbeat_time: float = 0
        
        # gRPC
        self._channel: Optional[grpc.aio.Channel] = None
        self._worker_stub = None
        self._connected = False
        
        # Running
        self._running = False
        self._tasks: List[asyncio.Task] = []
        
        # Docker availability
        self._docker_available: Optional[bool] = None
    
    def _check_docker_available(self) -> bool:
        """Check if Docker is available."""
        if self._docker_available is not None:
            return self._docker_available
        
        try:
            result = subprocess.run(
                ["docker", "version"],
                capture_output=True,
                timeout=5
            )
            self._docker_available = result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            self._docker_available = False
        
        return self._docker_available
    
    # ========== Resource Tracking ==========
    
    def get_available_resources(self) -> Dict[str, int]:
        """Get currently available resources."""
        return {
            "cpu_millis": self.cpu_millis - self.allocated_cpu,
            "memory_bytes": (self.memory_mb - self.allocated_memory_mb) * 1024 * 1024,
            "gpu_count": self.gpu_count - self.allocated_gpu_count,
            "gpu_memory_mb": self.gpu_memory_mb - self.allocated_gpu_memory_mb,
        }
    
    def can_allocate(self, resources: Dict[str, int]) -> bool:
        """Check if we can allocate the requested resources."""
        avail = self.get_available_resources()
        return (
            avail["cpu_millis"] >= resources.get("cpu_millis", 0) and
            avail["memory_bytes"] >= resources.get("memory_bytes", 0) and
            avail["gpu_count"] >= resources.get("gpu_count", 0) and
            avail["gpu_memory_mb"] >= resources.get("gpu_memory_mb", 0)
        )
    
    def allocate(self, resources: Dict[str, int]) -> bool:
        """Allocate resources for a task."""
        if not self.can_allocate(resources):
            return False
        
        self.allocated_cpu += resources.get("cpu_millis", 0)
        self.allocated_memory_mb += resources.get("memory_bytes", 0) // (1024 * 1024)
        self.allocated_gpu_count += resources.get("gpu_count", 0)
        self.allocated_gpu_memory_mb += resources.get("gpu_memory_mb", 0)
        return True
    
    def release(self, resources: Dict[str, int]) -> None:
        """Release resources when a task completes."""
        self.allocated_cpu = max(0, self.allocated_cpu - resources.get("cpu_millis", 0))
        self.allocated_memory_mb = max(0, self.allocated_memory_mb - resources.get("memory_bytes", 0) // (1024 * 1024))
        self.allocated_gpu_count = max(0, self.allocated_gpu_count - resources.get("gpu_count", 0))
        self.allocated_gpu_memory_mb = max(0, self.allocated_gpu_memory_mb - resources.get("gpu_memory_mb", 0))
    
    # ========== gRPC Connection ==========
    
    async def connect(self) -> bool:
        """Connect to the cluster via gRPC."""
        max_retries = 3
        retry_delay = 1.0
        
        for attempt in range(max_retries):
            try:
                from ..generated import aethergrid_pb2, aethergrid_pb2_grpc
                
                # Use 127.0.0.1 instead of localhost for faster resolution
                address = self.cluster_address.replace("localhost", "127.0.0.1")
                
                self._channel = grpc.aio.insecure_channel(address)
                self._worker_stub = aethergrid_pb2_grpc.WorkerServiceStub(self._channel)
                
                # Test connection with timeout
                await asyncio.wait_for(self._channel.channel_ready(), timeout=5.0)
                self._connected = True
                self.last_heartbeat_time = asyncio.get_event_loop().time()
                
                print(f"[Worker] Connected to cluster at {address}")
                return True
                
            except Exception as e:
                print(f"[Worker] Connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if self._channel:
                    await self._channel.close()
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
        
        self._connected = False
        return False
    
    async def register(self) -> bool:
        """Register this worker with the cluster."""
        if not self._connected:
            return False
        
        try:
            from ..generated import aethergrid_pb2
            
            request = aethergrid_pb2.RegisterNodeRequest(
                node_id=self.node_id,
                hostname=self.hostname,
                capacity=aethergrid_pb2.ResourceSpec(
                    cpu_millis=self.cpu_millis,
                    memory_bytes=self.memory_mb * 1024 * 1024,
                    gpu_count=self.gpu_count,
                    gpu_memory_mb=self.gpu_memory_mb,
                ),
                labels=self.labels,
                runtimes=[self.runtime],
            )
            
            response = await self._worker_stub.RegisterNode(request)
            
            if response.accepted:
                self.state = WorkerState.REGISTERED
                print(f"[Worker] Registered with cluster. Leader: {response.leader_id}")
                return True
            else:
                print(f"[Worker] Registration rejected")
                return False
                
        except Exception as e:
            print(f"[Worker] Registration failed: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from cluster."""
        self._connected = False
        self.state = WorkerState.DISCONNECTED
        
        # Kill all running tasks
        await self._kill_all_tasks("Cluster connection lost")
        
        if self._channel:
            await self._channel.close()
    
    # ========== Heartbeat ==========
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats."""
        while self._running:
            try:
                await self._send_heartbeat()
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[Worker] Heartbeat failed: {e}")
                await asyncio.sleep(1.0)
    
    async def _send_heartbeat(self) -> None:
        """Send heartbeat to leader."""
        if not self._connected or not self._worker_stub:
            return
        
        try:
            from ..generated import aethergrid_pb2
            
            avail = self.get_available_resources()
            
            request = aethergrid_pb2.HeartbeatRequest(
                node_id=self.node_id,
                running_tasks=[t.task_id for t in self.running_tasks.values()],
                available=aethergrid_pb2.ResourceSpec(
                    cpu_millis=avail["cpu_millis"],
                    memory_bytes=avail["memory_bytes"],
                    gpu_count=avail["gpu_count"],
                    gpu_memory_mb=avail["gpu_memory_mb"],
                ),
            )
            
            response = await self._worker_stub.Heartbeat(request)
            self.last_heartbeat_time = asyncio.get_event_loop().time()
            
            if response.acknowledged:
                print(f"[Worker] Heartbeat acknowledged")
                
        except Exception as e:
            print(f"[Worker] Heartbeat error: {e}")
    
    # ========== Task Execution ==========
    
    async def _execute_task(self, assignment: dict) -> None:
        """Execute a task assignment."""
        task_id = assignment.get("task_id", "")
        fencing_token = assignment.get("fencing_token", 0)
        lease_expiry_index = assignment.get("lease_expiry_index", 0)
        image = assignment.get("image", "")
        args = assignment.get("args", [])
        env = assignment.get("env", [])
        resources = assignment.get("resources", {})
        
        print(f"[Worker] Executing task {task_id}")
        
        # Check resource availability
        if not self.can_allocate(resources):
            await self._report_status(
                task_id=task_id,
                fencing_token=fencing_token,
                status="failed",
                error_message="Insufficient resources",
            )
            return
        
        # Allocate resources
        self.allocate(resources)
        
        # Create task record
        running_task = RunningTask(
            task_id=task_id,
            fencing_token=fencing_token,
            lease_expiry_index=lease_expiry_index,
            started_at=asyncio.get_event_loop().time(),
            resources=resources,
        )
        self.running_tasks[task_id] = running_task
        
        try:
            # Report START
            await self._report_status(
                task_id=task_id,
                fencing_token=fencing_token,
                status="running",
            )
            
            # Start the process
            process, container_id = await self._start_container(
                image=image,
                args=args,
                env=env,
                gpu_count=resources.get("gpu_count", 0),
                task_id=task_id,
            )
            
            running_task.process = process
            running_task.container_id = container_id
            
            # Start lease monitor
            lease_monitor = asyncio.create_task(
                self._monitor_lease(task_id, fencing_token)
            )
            
            # Wait for completion
            exit_code = await process.wait()
            
            # Cancel lease monitor
            lease_monitor.cancel()
            try:
                await lease_monitor
            except asyncio.CancelledError:
                pass
            
            # Report completion
            if exit_code == 0:
                await self._report_status(
                    task_id=task_id,
                    fencing_token=fencing_token,
                    status="succeeded",
                    exit_code=exit_code,
                )
            else:
                await self._report_status(
                    task_id=task_id,
                    fencing_token=fencing_token,
                    status="failed",
                    exit_code=exit_code,
                    error_message=f"Process exited with code {exit_code}",
                )
        
        except asyncio.CancelledError:
            # Task was cancelled
            await self._report_status(
                task_id=task_id,
                fencing_token=fencing_token,
                status="failed",
                error_message="Task cancelled (lease expired or connection lost)",
            )
        
        except Exception as e:
            await self._report_status(
                task_id=task_id,
                fencing_token=fencing_token,
                status="failed",
                error_message=str(e),
            )
        
        finally:
            # Release resources
            self.release(resources)
            
            # Remove from running tasks
            self.running_tasks.pop(task_id, None)
    
    async def _start_container(
        self,
        image: str,
        args: List[str],
        env: List[str],
        gpu_count: int,
        task_id: str,
    ) -> tuple:
        """Start a Docker container."""
        container_name = f"aether-{task_id.replace(':', '-')}"
        
        cmd = ["docker", "run", "--rm", "--name", container_name]
        
        # Environment
        for e in env:
            cmd.extend(["-e", e])
        
        # GPU support
        if gpu_count > 0 and self.gpu_count >= gpu_count:
            cmd.extend(["--gpus", str(gpu_count)])
            print(f"[Worker] Allocating {gpu_count} GPU(s) for task {task_id}")
        
        # Memory limit
        if self.memory_mb:
            cmd.extend(["--memory", f"{self.memory_mb}m"])
        
        cmd.append(image)
        cmd.extend(args)
        
        print(f"[Worker] Starting: {' '.join(cmd)}")
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        
        # Get container ID
        await asyncio.sleep(0.5)
        container_id = container_name
        
        return process, container_id
    
    async def _monitor_lease(self, task_id: str, fencing_token: int) -> None:
        """Monitor lease expiry for a task."""
        running_task = self.running_tasks.get(task_id)
        if not running_task:
            return
        
        while self._running:
            # Check lease expiry
            if self.cluster_log_index >= running_task.lease_expiry_index:
                print(f"[Worker] Lease expired for task {task_id}")
                await self._kill_task(task_id)
                return
            
            # Check connection
            if not self._connected:
                print(f"[Worker] Connection lost, killing task {task_id}")
                await self._kill_task(task_id)
                return
            
            await asyncio.sleep(1.0)
    
    async def _kill_task(self, task_id: str) -> None:
        """Kill a running task."""
        running_task = self.running_tasks.get(task_id)
        if not running_task:
            return
        
        # Kill Docker container
        if running_task.container_id:
            try:
                subprocess.run(
                    ["docker", "kill", running_task.container_id],
                    capture_output=True,
                    timeout=10
                )
                subprocess.run(
                    ["docker", "rm", "-f", running_task.container_id],
                    capture_output=True,
                    timeout=10
                )
            except Exception as e:
                print(f"[Worker] Error killing container: {e}")
        
        # Kill process
        if running_task.process and running_task.process.returncode is None:
            try:
                running_task.process.terminate()
                await asyncio.sleep(1)
                if running_task.process.returncode is None:
                    running_task.process.kill()
            except Exception:
                pass
    
    async def _kill_all_tasks(self, reason: str) -> None:
        """Kill all running tasks."""
        print(f"[Worker] Killing all tasks: {reason}")
        for task_id in list(self.running_tasks.keys()):
            await self._kill_task(task_id)
    
    # ========== Status Reporting ==========
    
    async def _report_status(
        self,
        task_id: str,
        fencing_token: int,
        status: str,
        exit_code: int = 0,
        error_message: str = "",
    ) -> bool:
        """Report task status to leader."""
        if not self._connected or not self._worker_stub:
            return False
        
        try:
            from ..generated import aethergrid_pb2
            
            # Map status string to enum
            status_map = {
                "running": aethergrid_pb2.TASK_STATUS_RUNNING,
                "succeeded": aethergrid_pb2.TASK_STATUS_SUCCEEDED,
                "failed": aethergrid_pb2.TASK_STATUS_FAILED,
            }
            
            # Parse task_id
            parts = task_id.split(":")
            if len(parts) == 2:
                tid = aethergrid_pb2.TaskID(
                    shard=int(parts[0]),
                    sequence=int(parts[1]),
                )
            else:
                return False
            
            request = aethergrid_pb2.TaskStatusReport(
                task_id=tid,
                fencing_token=fencing_token,
                status=status_map.get(status, aethergrid_pb2.TASK_STATUS_RUNNING),
                exit_code=exit_code,
                error_message=error_message,
            )
            
            response = await self._worker_stub.ReportStatus(request)
            
            if response.accepted:
                print(f"[Worker] Status reported: {status}")
                return True
            else:
                print(f"[Worker] Status rejected: {response.rejection_reason}")
                return False
                
        except Exception as e:
            print(f"[Worker] Failed to report status: {e}")
            return False
    
    # ========== Main Loop ==========
    
    async def run(self) -> None:
        """Main event loop."""
        self._running = True
        
        # Setup signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: self.stop())
        
        # Connect to cluster
        if not await self.connect():
            print("[Worker] Failed to connect to cluster")
            return
        
        # Register with cluster
        if not await self.register():
            print("[Worker] Failed to register with cluster")
            return
        
        self.state = WorkerState.RUNNING
        print(f"[Worker] Running with {self.gpu_count} GPUs, {self.gpu_memory_mb}MB VRAM")
        
        # Start heartbeat loop
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._tasks.append(heartbeat_task)
        
        # Wait for shutdown
        try:
            while self._running:
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            pass
        
        # Cleanup
        for task in self._tasks:
            task.cancel()
        
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self._kill_all_tasks("Worker shutting down")
        await self.disconnect()
    
    def stop(self) -> None:
        """Stop the worker."""
        self._running = False
        self.state = WorkerState.STOPPED


# ============== CLI Entry Point ==============

def run_worker():
    """Run a standalone worker from CLI."""
    import argparse
    
    parser = argparse.ArgumentParser(description="AetherGrid Worker Daemon")
    parser.add_argument("--node-id", required=True, help="Unique node ID")
    parser.add_argument("--cluster", default="localhost:50051", help="Cluster address (host:port)")
    parser.add_argument("--hostname", default=None, help="Hostname (auto-detected if not set)")
    parser.add_argument("--cpu", type=int, default=4000, help="CPU capacity in milli-cores")
    parser.add_argument("--memory", type=int, default=8192, help="Memory capacity in MB")
    parser.add_argument("--gpu-count", type=int, default=0, help="Number of GPUs")
    parser.add_argument("--gpu-memory", type=int, default=0, help="GPU memory in MB")
    parser.add_argument("--runtime", default="docker", choices=["docker", "process"], help="Runtime type")
    parser.add_argument("--labels", default="", help="Comma-separated labels (key=value)")
    
    args = parser.parse_args()
    
    # Parse labels
    labels = {}
    if args.labels:
        for label in args.labels.split(","):
            if "=" in label:
                k, v = label.split("=", 1)
                labels[k.strip()] = v.strip()
    
    # Create and run worker
    daemon = WorkerDaemon(
        node_id=args.node_id,
        cluster_address=args.cluster,
        hostname=args.hostname,
        cpu_millis=args.cpu,
        memory_mb=args.memory,
        gpu_count=args.gpu_count,
        gpu_memory_mb=args.gpu_memory,
        labels=labels,
        runtime=args.runtime,
    )
    
    try:
        asyncio.run(daemon.run())
    except KeyboardInterrupt:
        daemon.stop()


if __name__ == "__main__":
    run_worker()
