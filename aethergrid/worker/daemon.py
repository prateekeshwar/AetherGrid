#!/usr/bin/env python3
"""AetherGrid v2.0 - Worker Daemon with gRPC Streaming.

The worker daemon:
1. Connects to the cluster via gRPC
2. Streams task assignments in real-time
3. Executes tasks with subprocess isolation
4. Tracks lease expiry and kills tasks on connection loss
5. Reports status with fencing tokens

Worker Safety:
- Tracks lease_expiry_index from leader
- Kills subprocess if cluster connection lost
- Validates fencing tokens before status reports
"""

import asyncio
import subprocess
import os
import signal
import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Callable
from enum import Enum
import socket
import struct


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
    process: asyncio.subprocess.Process
    started_at: float
    log_index_at_start: int
    
    # stdout/stderr buffers
    stdout_buffer: List[bytes] = field(default_factory=list)
    stderr_buffer: List[bytes] = field(default_factory=list)


class WorkerDaemon:
    """
    Worker daemon with gRPC streaming and safety features.
    
    Key features:
    1. Real-time task assignment via gRPC streaming
    2. Lease tracking - kills tasks when lease expires
    3. Connection monitoring - kills tasks on disconnect
    4. Fencing token validation
    """
    
    # Safety parameters
    HEARTBEAT_INTERVAL = 5.0        # Seconds between heartbeats
    CONNECTION_TIMEOUT = 15.0       # Seconds before considering disconnected
    LEASE_RENEWAL_MARGIN = 200      # Renew lease when this many entries remain
    
    def __init__(
        self,
        node_id: str,
        cluster_address: str,
        hostname: Optional[str] = None,
        capacity: Optional[Dict[str, int]] = None,
        labels: Optional[Dict[str, str]] = None,
        runtime: str = "docker",
        data_dir: str = "/tmp/aethergrid/worker",
    ):
        self.node_id = node_id
        self.cluster_address = cluster_address
        self.hostname = hostname or socket.gethostname()
        self.capacity = capacity or {"cpu_millis": 4000, "memory_bytes": 8589934592}
        self.labels = labels or {}
        self.runtime = runtime
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # State
        self.state = WorkerState.INITIALIZING
        self.running_tasks: Dict[str, RunningTask] = {}
        
        # Lease tracking
        self.cluster_log_index: int = 0      # Last known cluster log index
        self.last_heartbeat_time: float = 0  # Time of last successful heartbeat
        self.last_connection_time: float = 0 # Time of last successful connection
        
        # gRPC connection
        self._channel = None
        self._connected = False
        
        # Running flag
        self._running = False
        self._tasks: List[asyncio.Task] = []
        
        # Assignment stream
        self._assignment_queue: asyncio.Queue = asyncio.Queue()
    
    # ========== Connection Management ==========
    
    async def connect(self) -> bool:
        """Connect to the cluster."""
        try:
            # Simulate gRPC connection (in production, use actual gRPC)
            self._connected = True
            self.last_connection_time = asyncio.get_event_loop().time()
            self.state = WorkerState.REGISTERED
            
            # Register with cluster
            await self._register()
            
            return True
        except Exception as e:
            print(f"Failed to connect: {e}")
            self._connected = False
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from cluster and kill all running tasks."""
        self._connected = False
        self.state = WorkerState.DISCONNECTED
        
        # CRITICAL: Kill all running tasks on disconnect
        await self._kill_all_tasks("Cluster connection lost")
    
    async def _register(self) -> bool:
        """Register this worker with the cluster."""
        # In production, this would be a gRPC call
        registration = {
            "node_id": self.node_id,
            "hostname": self.hostname,
            "capacity": self.capacity,
            "labels": self.labels,
            "runtimes": [self.runtime],
        }
        
        # Simulate successful registration
        self.state = WorkerState.REGISTERED
        return True
    
    # ========== Task Assignment Streaming ==========
    
    async def _stream_assignments(self) -> None:
        """
        Stream task assignments from the cluster.
        
        This is the main loop for receiving work.
        Assignments include:
        - Task definition
        - Fencing token
        - Lease expiry index
        """
        while self._running and self._connected:
            try:
                # In production, this would be a gRPC streaming call
                # For now, we'll use a queue that can be populated externally
                assignment = await asyncio.wait_for(
                    self._assignment_queue.get(),
                    timeout=1.0
                )
                
                await self._handle_assignment(assignment)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"Error in assignment stream: {e}")
                await asyncio.sleep(1.0)
    
    async def _handle_assignment(self, assignment: dict) -> None:
        """Handle a task assignment from the cluster."""
        task_id = assignment.get("task_id", "")
        fencing_token = assignment.get("fencing_token", 0)
        lease_expiry_index = assignment.get("lease_expiry_index", 0)
        
        print(f"Received assignment: task={task_id}, token={fencing_token}, lease={lease_expiry_index}")
        
        # Check if we already have this task
        if task_id in self.running_tasks:
            existing = self.running_tasks[task_id]
            
            # Check fencing token
            if fencing_token < existing.fencing_token:
                # Stale assignment - ignore
                print(f"Ignoring stale assignment for {task_id}")
                return
            
            if fencing_token > existing.fencing_token:
                # New assignment - kill existing task
                await self._kill_task(task_id, "Reassigned with new fencing token")
        
        # Execute the task
        asyncio.create_task(self._execute_task(assignment))
    
    # ========== Task Execution ==========
    
    async def _execute_task(self, assignment: dict) -> None:
        """Execute a task assignment."""
        task_id = assignment.get("task_id", "")
        fencing_token = assignment.get("fencing_token", 0)
        lease_expiry_index = assignment.get("lease_expiry_index", 0)
        image = assignment.get("image", "")
        args = assignment.get("args", [])
        env = assignment.get("env", [])
        
        print(f"Executing task {task_id}")
        
        try:
            # Start the process
            process = await self._start_process(image, args, env)
            
            # Create running task record
            running_task = RunningTask(
                task_id=task_id,
                fencing_token=fencing_token,
                lease_expiry_index=lease_expiry_index,
                process=process,
                started_at=asyncio.get_event_loop().time(),
                log_index_at_start=self.cluster_log_index,
            )
            self.running_tasks[task_id] = running_task
            
            # Report START with fencing token
            await self._report_status(
                task_id=task_id,
                fencing_token=fencing_token,
                status="running",
            )
            
            # Wait for completion with lease monitoring
            exit_code = await self._wait_with_lease_monitoring(task_id, process)
            
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
            # Task was cancelled (lease expired or connection lost)
            await self._report_status(
                task_id=task_id,
                fencing_token=fencing_token,
                status="orphaned",
                error_message="Task killed due to lease expiry or connection loss",
            )
        
        except Exception as e:
            await self._report_status(
                task_id=task_id,
                fencing_token=fencing_token,
                status="failed",
                error_message=str(e),
            )
        
        finally:
            # Remove from running tasks
            self.running_tasks.pop(task_id, None)
    
    async def _start_process(
        self, 
        image: str, 
        args: List[str], 
        env: List[str]
    ) -> asyncio.subprocess.Process:
        """Start a process for the task."""
        if self.runtime == "docker":
            cmd = ["docker", "run", "--rm"]
            for e in env:
                cmd.extend(["-e", e])
            cmd.append(image)
            cmd.extend(args)
        else:
            cmd = [image] + args
        
        env_dict = os.environ.copy()
        for e in env:
            if "=" in e:
                key, value = e.split("=", 1)
                env_dict[key] = value
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env_dict,
        )
        
        return process
    
    async def _wait_with_lease_monitoring(
        self,
        task_id: str,
        process: asyncio.subprocess.Process,
    ) -> int:
        """
        Wait for process with lease monitoring.
        
        CRITICAL: This monitors the lease and kills the process if:
        1. Lease expires (cluster_log_index >= lease_expiry_index)
        2. Connection to cluster is lost
        """
        running_task = self.running_tasks.get(task_id)
        if not running_task:
            return -1
        
        while True:
            # Check if process is done
            if process.returncode is not None:
                return process.returncode
            
            # Check connection
            if not self._connected:
                print(f"Connection lost, killing task {task_id}")
                await self._kill_process(process)
                raise asyncio.CancelledError("Connection lost")
            
            # Check lease expiry
            if self.cluster_log_index >= running_task.lease_expiry_index:
                print(f"Lease expired for task {task_id}")
                await self._kill_process(process)
                raise asyncio.CancelledError("Lease expired")
            
            # Wait a bit before checking again
            await asyncio.sleep(0.1)
    
    async def _kill_process(self, process: asyncio.subprocess.Process) -> None:
        """Kill a process gracefully."""
        try:
            process.terminate()
            await asyncio.sleep(1)
            if process.returncode is None:
                process.kill()
                await process.wait()
        except Exception:
            pass
    
    async def _kill_task(self, task_id: str, reason: str) -> None:
        """Kill a specific running task."""
        running_task = self.running_tasks.get(task_id)
        if running_task and running_task.process:
            print(f"Killing task {task_id}: {reason}")
            await self._kill_process(running_task.process)
    
    async def _kill_all_tasks(self, reason: str) -> None:
        """Kill all running tasks."""
        print(f"Killing all tasks: {reason}")
        for task_id in list(self.running_tasks.keys()):
            await self._kill_task(task_id, reason)
    
    # ========== Status Reporting ==========
    
    async def _report_status(
        self,
        task_id: str,
        fencing_token: int,
        status: str,
        exit_code: int = 0,
        error_message: str = "",
    ) -> bool:
        """
        Report task status to the cluster.
        
        CRITICAL: The fencing token MUST be valid for the report to be accepted.
        """
        if not self._connected:
            return False
        
        report = {
            "task_id": task_id,
            "node_id": self.node_id,
            "fencing_token": fencing_token,
            "status": status,
            "exit_code": exit_code,
            "error_message": error_message,
        }
        
        # In production, this would be a gRPC call
        print(f"Reporting status: {report}")
        return True
    
    # ========== Heartbeat and Lease Management ==========
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to the cluster."""
        while self._running:
            try:
                await self._send_heartbeat()
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            except Exception as e:
                print(f"Heartbeat failed: {e}")
                await asyncio.sleep(1.0)
    
    async def _send_heartbeat(self) -> None:
        """Send heartbeat to cluster and receive current log index."""
        if not self._connected:
            return
        
        heartbeat = {
            "node_id": self.node_id,
            "running_tasks": list(self.running_tasks.keys()),
            "current_log_index": self.cluster_log_index,
        }
        
        # In production, this would be a gRPC call
        # For now, simulate receiving the cluster log index
        # response = await stub.Heartbeat(heartbeat)
        # self.cluster_log_index = response.cluster_log_index
        
        self.last_heartbeat_time = asyncio.get_event_loop().time()
    
    async def _lease_renewal_loop(self) -> None:
        """Renew leases for running tasks before they expire."""
        while self._running:
            try:
                for task_id, running_task in list(self.running_tasks.items()):
                    # Check if lease needs renewal
                    remaining = running_task.lease_expiry_index - self.cluster_log_index
                    
                    if remaining < self.LEASE_RENEWAL_MARGIN:
                        await self._renew_lease(task_id, running_task)
                
                await asyncio.sleep(1.0)
            
            except Exception as e:
                print(f"Lease renewal error: {e}")
    
    async def _renew_lease(self, task_id: str, running_task: RunningTask) -> bool:
        """Request lease renewal for a task."""
        if not self._connected:
            return False
        
        request = {
            "task_id": task_id,
            "node_id": self.node_id,
            "fencing_token": running_task.fencing_token,
            "additional_entries": 1000,  # Extend by 1000 log entries
        }
        
        # In production, this would be a gRPC call
        print(f"Requesting lease renewal for {task_id}")
        return True
    
    async def _connection_monitor(self) -> None:
        """Monitor connection health and kill tasks if connection lost."""
        while self._running:
            now = asyncio.get_event_loop().time()
            
            # Check if connection has timed out
            if self._connected:
                time_since_heartbeat = now - self.last_heartbeat_time
                
                if time_since_heartbeat > self.CONNECTION_TIMEOUT:
                    print("Connection timeout detected")
                    await self.disconnect()
            
            await asyncio.sleep(1.0)
    
    # ========== Public API ==========
    
    def submit_assignment(self, assignment: dict) -> None:
        """Submit an assignment to this worker (for testing)."""
        self._assignment_queue.put_nowait(assignment)
    
    def update_cluster_log_index(self, index: int) -> None:
        """Update the cluster log index (for testing)."""
        self.cluster_log_index = index
    
    # ========== Main Loop ==========
    
    async def run(self) -> None:
        """Main event loop for the worker daemon."""
        self._running = True
        
        # Connect to cluster
        while not await self.connect():
            print("Retrying connection in 5 seconds...")
            await asyncio.sleep(5.0)
        
        self.state = WorkerState.RUNNING
        
        # Start background tasks
        self._tasks = [
            asyncio.create_task(self._stream_assignments()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._lease_renewal_loop()),
            asyncio.create_task(self._connection_monitor()),
        ]
        
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
        
        # Kill all running tasks
        await self._kill_all_tasks("Worker shutting down")
        
        # Disconnect
        await self.disconnect()
    
    def stop(self) -> None:
        """Stop the worker daemon."""
        self._running = False


# ============== Standalone Entry Point ==============

async def main():
    """Run a worker daemon standalone."""
    import argparse
    
    parser = argparse.ArgumentParser(description="AetherGrid Worker Daemon")
    parser.add_argument("--node-id", required=True, help="Unique node ID")
    parser.add_argument("--cluster", default="localhost:50051", help="Cluster address")
    parser.add_argument("--hostname", default=None, help="Hostname")
    parser.add_argument("--runtime", default="docker", help="Runtime (docker/process)")
    parser.add_argument("--data-dir", default="/tmp/aethergrid/worker", help="Data directory")
    
    args = parser.parse_args()
    
    daemon = WorkerDaemon(
        node_id=args.node_id,
        cluster_address=args.cluster,
        hostname=args.hostname,
        runtime=args.runtime,
        data_dir=args.data_dir,
    )
    
    try:
        await daemon.run()
    except KeyboardInterrupt:
        daemon.stop()


if __name__ == "__main__":
    asyncio.run(main())
