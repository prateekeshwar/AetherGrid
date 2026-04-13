"""AetherGrid v2.0 - Worker Agent with Fencing Token Validation.

The Worker Agent runs on worker nodes and:
1. Receives task assignments from the leader
2. Validates fencing tokens before executing tasks
3. Reports task status with fencing tokens
4. Handles lease renewals to keep tasks running

Key security feature:
- Workers must present valid fencing tokens to update task state
- Stale tokens are rejected, preventing zombie workers
"""

import asyncio
import subprocess
import os
import signal
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Callable
from enum import Enum


@dataclass
class TaskHandle:
    """Handle for a running task."""
    task_id: str
    fencing_token: int
    lease_expiry_index: int
    process: Optional[subprocess.Popen] = None
    started_at: float = 0.0
    stdout_buffer: List[bytes] = field(default_factory=list)
    stderr_buffer: List[bytes] = field(default_factory=list)


class WorkerAgent:
    """
    Worker agent that executes tasks with fencing token validation.
    
    The agent:
    1. Registers with the cluster
    2. Receives task assignments
    3. Validates fencing tokens before execution
    4. Reports status with fencing tokens
    5. Handles lease renewals
    
    Fencing Token Protocol:
    - Each task assignment includes a fencing token
    - Worker must present the SAME token when reporting status
    - If token doesn't match, the report is rejected
    - This prevents zombie workers from corrupting state
    """
    
    def __init__(
        self,
        node_id: str,
        hostname: str,
        capacity: Dict[str, int],
        send_to_leader: Callable[[dict], None],
        labels: Optional[Dict[str, str]] = None,
        runtime: str = "docker",
    ):
        self.node_id = node_id
        self.hostname = hostname
        self.capacity = capacity
        self.send_to_leader = send_to_leader
        self.labels = labels or {}
        self.runtime = runtime
        
        # Running tasks (task_id -> TaskHandle)
        self.running_tasks: Dict[str, TaskHandle] = {}
        
        # Task queue
        self.task_queue: asyncio.Queue = asyncio.Queue()
        
        # Current log index (for lease calculations)
        self.current_log_index: int = 0
        
        # Running flag
        self._running: bool = False
    
    # ========== Task Assignment ==========
    
    async def receive_assignment(self, assignment: dict) -> bool:
        """
        Receive a task assignment from the leader.
        
        Args:
            assignment: Task assignment with fencing token
        
        Returns:
            True if assignment accepted, False otherwise
        """
        task_id = assignment.get("task_id", "")
        fencing_token = assignment.get("fencing_token", 0)
        lease_expiry_index = assignment.get("lease_expiry_index", 0)
        
        # Check if we already have this task
        if task_id in self.running_tasks:
            existing = self.running_tasks[task_id]
            
            # Check fencing token
            if fencing_token < existing.fencing_token:
                # Stale assignment - reject
                return False
            
            if fencing_token > existing.fencing_token:
                # New assignment - kill existing task
                await self._kill_task(task_id)
        
        # Add to queue
        await self.task_queue.put(assignment)
        return True
    
    # ========== Task Execution ==========
    
    async def _execute_task(self, assignment: dict) -> None:
        """Execute a task assignment."""
        task_id = assignment.get("task_id", "")
        fencing_token = assignment.get("fencing_token", 0)
        lease_expiry_index = assignment.get("lease_expiry_index", 0)
        image = assignment.get("image", "")
        args = assignment.get("args", [])
        env = assignment.get("env", [])
        
        # Create handle
        handle = TaskHandle(
            task_id=task_id,
            fencing_token=fencing_token,
            lease_expiry_index=lease_expiry_index,
            started_at=asyncio.get_event_loop().time(),
        )
        self.running_tasks[task_id] = handle
        
        # Report START with fencing token
        await self._report_status(
            task_id=task_id,
            fencing_token=fencing_token,
            status="running",
        )
        
        try:
            # Execute task based on runtime
            if self.runtime == "docker":
                process = await self._run_docker(image, args, env)
            elif self.runtime == "process":
                process = await self._run_process(image, args, env)
            else:
                raise ValueError(f"Unknown runtime: {self.runtime}")
            
            handle.process = process
            
            # Wait for completion
            exit_code = await process.wait()
            
            # Report completion with fencing token
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
        
        except Exception as e:
            # Report failure with fencing token
            await self._report_status(
                task_id=task_id,
                fencing_token=fencing_token,
                status="failed",
                error_message=str(e),
            )
        
        finally:
            # Remove from running tasks
            if task_id in self.running_tasks:
                del self.running_tasks[task_id]
    
    async def _run_docker(
        self, 
        image: str, 
        args: List[str], 
        env: List[str]
    ) -> asyncio.subprocess.Process:
        """Run task in Docker container."""
        cmd = ["docker", "run", "--rm"]
        
        # Add environment variables
        for e in env:
            cmd.extend(["-e", e])
        
        cmd.append(image)
        cmd.extend(args)
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        
        return process
    
    async def _run_process(
        self, 
        command: str, 
        args: List[str], 
        env: List[str]
    ) -> asyncio.subprocess.Process:
        """Run task as native process."""
        env_dict = os.environ.copy()
        for e in env:
            if "=" in e:
                key, value = e.split("=", 1)
                env_dict[key] = value
        
        process = await asyncio.create_subprocess_exec(
            command,
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env_dict,
        )
        
        return process
    
    # ========== Status Reporting (with Fencing Tokens) ==========
    
    async def _report_status(
        self,
        task_id: str,
        fencing_token: int,
        status: str,
        exit_code: int = 0,
        error_message: str = "",
    ) -> bool:
        """
        Report task status to the leader with fencing token.
        
        CRITICAL: The fencing token MUST match the current token
        for the task, or the report will be rejected.
        
        This prevents zombie workers from corrupting state.
        """
        report = {
            "type": "TaskStatusReport",
            "task_id": task_id,
            "fencing_token": fencing_token,  # CRITICAL for zombie protection
            "node_id": self.node_id,
            "status": status,
            "exit_code": exit_code,
            "error_message": error_message,
            "timestamp": asyncio.get_event_loop().time(),
        }
        
        self.send_to_leader(report)
        return True
    
    # ========== Lease Management ==========
    
    async def renew_lease(
        self, 
        task_id: str, 
        additional_entries: int = 1000
    ) -> bool:
        """
        Request lease renewal for a running task.
        
        Must be called before lease expires.
        Uses the current fencing token.
        """
        handle = self.running_tasks.get(task_id)
        if not handle:
            return False
        
        request = {
            "type": "RenewLease",
            "task_id": task_id,
            "fencing_token": handle.fencing_token,  # Must match
            "node_id": self.node_id,
            "additional_entries": additional_entries,
        }
        
        self.send_to_leader(request)
        return True
    
    def update_log_index(self, index: int) -> List[str]:
        """
        Update current log index and check for expired leases.
        
        Returns list of task_ids with expired leases.
        """
        self.current_log_index = index
        
        expired = []
        for task_id, handle in self.running_tasks.items():
            if self.current_log_index >= handle.lease_expiry_index:
                expired.append(task_id)
        
        return expired
    
    # ========== Task Control ==========
    
    async def _kill_task(self, task_id: str) -> None:
        """Kill a running task."""
        handle = self.running_tasks.get(task_id)
        if handle and handle.process:
            try:
                handle.process.terminate()
                await asyncio.sleep(1)
                if handle.process.returncode is None:
                    handle.process.kill()
            except Exception:
                pass
    
    async def cancel_task(
        self, 
        task_id: str, 
        new_fencing_token: int
    ) -> bool:
        """
        Cancel a running task.
        
        The new_fencing_token is used to invalidate any future
        status reports from this task.
        """
        handle = self.running_tasks.get(task_id)
        if not handle:
            return False
        
        # Update fencing token (invalidates future reports)
        handle.fencing_token = new_fencing_token
        
        # Kill the task
        await self._kill_task(task_id)
        
        return True
    
    # ========== Heartbeat ==========
    
    async def send_heartbeat(self) -> None:
        """Send heartbeat to leader with running tasks."""
        running_task_ids = list(self.running_tasks.keys())
        
        heartbeat = {
            "type": "NodeHeartbeat",
            "node_id": self.node_id,
            "running_tasks": running_task_ids,
            "current_log_index": self.current_log_index,
            "health": "healthy",
        }
        
        self.send_to_leader(heartbeat)
    
    # ========== Main Loop ==========
    
    async def run(self) -> None:
        """Main event loop for the worker agent."""
        self._running = True
        
        # Register with cluster
        await self._register()
        
        # Start heartbeat task
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        # Start lease check task
        lease_task = asyncio.create_task(self._lease_check_loop())
        
        # Process task assignments
        while self._running:
            try:
                assignment = await asyncio.wait_for(
                    self.task_queue.get(),
                    timeout=1.0
                )
                
                # Execute task
                asyncio.create_task(self._execute_task(assignment))
            
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"Error processing task: {e}")
        
        # Cleanup
        heartbeat_task.cancel()
        lease_task.cancel()
        
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass
        
        try:
            await lease_task
        except asyncio.CancelledError:
            pass
    
    async def _register(self) -> None:
        """Register this worker with the cluster."""
        registration = {
            "type": "RegisterNode",
            "node_id": self.node_id,
            "hostname": self.hostname,
            "capacity": self.capacity,
            "labels": self.labels,
            "runtimes": [self.runtime],
        }
        
        self.send_to_leader(registration)
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats."""
        while self._running:
            await self.send_heartbeat()
            await asyncio.sleep(5.0)  # Heartbeat every 5 seconds
    
    async def _lease_check_loop(self) -> None:
        """Check for expiring leases and renew them."""
        while self._running:
            # Check for tasks with leases expiring soon
            for task_id, handle in self.running_tasks.items():
                # Renew if lease expires in next 500 entries
                if self.current_log_index + 500 >= handle.lease_expiry_index:
                    await self.renew_lease(task_id)
            
            await asyncio.sleep(1.0)
    
    def stop(self) -> None:
        """Stop the worker agent."""
        self._running = False
        
        # Kill all running tasks
        for task_id in list(self.running_tasks.keys()):
            asyncio.create_task(self._kill_task(task_id))


class FencingTokenValidator:
    """
    Validates fencing tokens for the leader.
    
    Used by the leader to validate status reports from workers.
    """
    
    @staticmethod
    def validate(
        task_current_token: int,
        provided_token: int,
    ) -> tuple[bool, str]:
        """
        Validate a fencing token.
        
        Returns (is_valid, error_message).
        
        Rules:
        - Token must match exactly
        - Stale tokens (< current) are rejected
        - Future tokens (> current) are rejected
        """
        if provided_token < task_current_token:
            return False, f"Stale fencing token: provided={provided_token}, current={task_current_token}"
        
        if provided_token > task_current_token:
            return False, f"Future fencing token: provided={provided_token}, current={task_current_token}"
        
        return True, ""
    
    @staticmethod
    def is_zombie_worker(
        task_current_token: int,
        provided_token: int,
    ) -> bool:
        """
        Check if a worker is a zombie (using stale token).
        
        A zombie worker is one that continues to operate after
        its task has been reassigned (and thus has a new token).
        """
        return provided_token < task_current_token
