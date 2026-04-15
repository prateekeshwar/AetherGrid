"""AetherGrid v2.0 - State Machine with Fencing Tokens and Log-Based Leases.

This module implements the core state machine for the Decentralized OS,
including:

1. **Fencing Tokens**: Monotonically increasing tokens that prevent zombie
   workers from making stale updates. Each task assignment gets a new token,
   and workers must present the current token to make state changes.

2. **Log-Based Leases**: Deterministic time using log index instead of
   wall-clock time. Leases expire at a specific log index, making them
   consistent across all nodes.

3. **Zombie Worker Protection**: Workers with outdated fencing tokens
   cannot update task state, preventing split-brain scenarios.

NO datetime.now() OR time.time() IS USED IN THIS MODULE.
All time is measured in log indices for determinism.
"""

import json
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict


class TaskStatus(Enum):
    """Task lifecycle states."""
    PENDING = "pending"          # Submitted, waiting for scheduling
    SCHEDULED = "scheduled"      # Assigned to a node, not yet running
    RUNNING = "running"          # Currently executing
    SUCCEEDED = "succeeded"      # Completed successfully
    FAILED = "failed"            # Failed with error
    CANCELLED = "cancelled"      # Cancelled by user
    TIMEOUT = "timeout"          # Exceeded TTL
    ORPHANED = "orphaned"        # Worker died, needs rescheduling


class NodeHealth(Enum):
    """Node health states."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class TaskID:
    """Unique identifier for a task."""
    shard: int
    sequence: int
    
    def __hash__(self):
        return hash((self.shard, self.sequence))
    
    def __eq__(self, other):
        if not isinstance(other, TaskID):
            return False
        return self.shard == other.shard and self.sequence == other.sequence
    
    def __lt__(self, other):
        if not isinstance(other, TaskID):
            return NotImplemented
        return (self.shard, self.sequence) < (other.shard, other.sequence)
    
    def __repr__(self):
        return f"TaskID({self.shard}:{self.sequence})"
    
    def to_dict(self) -> dict:
        return {"shard": self.shard, "sequence": self.sequence}
    
    @classmethod
    def from_dict(cls, data: dict) -> "TaskID":
        return cls(shard=data["shard"], sequence=data["sequence"])


@dataclass
class NodeID:
    """Unique identifier for a node."""
    id: str
    
    def __hash__(self):
        return hash(self.id)
    
    def __eq__(self, other):
        if not isinstance(other, NodeID):
            return False
        return self.id == other.id
    
    def __repr__(self):
        return f"NodeID({self.id})"
    
    def to_dict(self) -> dict:
        return {"id": self.id}
    
    @classmethod
    def from_dict(cls, data: dict) -> "NodeID":
        return cls(id=data["id"])


@dataclass
class ResourceSpec:
    """Resource specification."""
    cpu_millis: int = 0
    memory_bytes: int = 0
    gpu_count: int = 0
    extended: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            "cpu_millis": self.cpu_millis,
            "memory_bytes": self.memory_bytes,
            "gpu_count": self.gpu_count,
            "extended": self.extended,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "ResourceSpec":
        return cls(
            cpu_millis=data.get("cpu_millis", 0),
            memory_bytes=data.get("memory_bytes", 0),
            gpu_count=data.get("gpu_count", 0),
            extended=data.get("extended", {}),
        )


@dataclass
class Task:
    """
    Task definition with fencing token and log-based lease.
    
    Key fields for zombie worker protection:
    - fencing_token: Monotonically increasing token for this task
    - lease_expiry_index: Log index when lease expires
    - created_at_index: Log index when task was created (deterministic time)
    """
    id: TaskID
    name: str
    namespace: str
    
    # Spec
    image: str = ""
    args: List[str] = field(default_factory=list)
    env: List[str] = field(default_factory=list)
    resources: ResourceSpec = field(default_factory=ResourceSpec)
    ttl_log_entries: int = 0  # TTL in log entries (deterministic time)
    max_retries: int = 3
    
    # Status
    status: TaskStatus = TaskStatus.PENDING
    assigned_node: Optional[NodeID] = None
    
    # Deterministic time (log indices, NOT wall clock)
    created_at_index: int = 0
    scheduled_at_index: int = 0
    started_at_index: int = 0
    finished_at_index: int = 0
    
    exit_code: int = 0
    error_message: str = ""
    retry_count: int = 0
    
    # Zombie worker protection
    fencing_token: int = 0           # Monotonically increasing per task
    lease_expiry_index: int = 0      # Log index when lease expires
    
    # Metadata
    labels: Dict[str, str] = field(default_factory=dict)
    submitted_by: str = ""
    
    def to_dict(self) -> dict:
        return {
            "id": self.id.to_dict(),
            "name": self.name,
            "namespace": self.namespace,
            "image": self.image,
            "args": self.args,
            "env": self.env,
            "resources": self.resources.to_dict(),
            "ttl_log_entries": self.ttl_log_entries,
            "max_retries": self.max_retries,
            "status": self.status.value,
            "assigned_node": self.assigned_node.to_dict() if self.assigned_node else None,
            "created_at_index": self.created_at_index,
            "scheduled_at_index": self.scheduled_at_index,
            "started_at_index": self.started_at_index,
            "finished_at_index": self.finished_at_index,
            "exit_code": self.exit_code,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "fencing_token": self.fencing_token,
            "lease_expiry_index": self.lease_expiry_index,
            "labels": self.labels,
            "submitted_by": self.submitted_by,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Task":
        return cls(
            id=TaskID.from_dict(data["id"]),
            name=data["name"],
            namespace=data["namespace"],
            image=data.get("image", ""),
            args=data.get("args", []),
            env=data.get("env", []),
            resources=ResourceSpec.from_dict(data.get("resources", {})),
            ttl_log_entries=data.get("ttl_log_entries", 0),
            max_retries=data.get("max_retries", 3),
            status=TaskStatus(data.get("status", "pending")),
            assigned_node=NodeID.from_dict(data["assigned_node"]) if data.get("assigned_node") else None,
            created_at_index=data.get("created_at_index", 0),
            scheduled_at_index=data.get("scheduled_at_index", 0),
            started_at_index=data.get("started_at_index", 0),
            finished_at_index=data.get("finished_at_index", 0),
            exit_code=data.get("exit_code", 0),
            error_message=data.get("error_message", ""),
            retry_count=data.get("retry_count", 0),
            fencing_token=data.get("fencing_token", 0),
            lease_expiry_index=data.get("lease_expiry_index", 0),
            labels=data.get("labels", {}),
            submitted_by=data.get("submitted_by", ""),
        )


@dataclass
class Node:
    """Worker node in the cluster."""
    id: NodeID
    hostname: str
    capacity: ResourceSpec
    allocated: ResourceSpec
    labels: Dict[str, str]
    health: NodeHealth
    last_heartbeat_index: int  # Log index of last heartbeat
    running_tasks: Set[TaskID]
    
    def to_dict(self) -> dict:
        return {
            "id": self.id.to_dict(),
            "hostname": self.hostname,
            "capacity": self.capacity.to_dict(),
            "allocated": self.allocated.to_dict(),
            "labels": self.labels,
            "health": self.health.value,
            "last_heartbeat_index": self.last_heartbeat_index,
            "running_tasks": [t.to_dict() for t in self.running_tasks],
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Node":
        return cls(
            id=NodeID.from_dict(data["id"]),
            hostname=data["hostname"],
            capacity=ResourceSpec.from_dict(data["capacity"]),
            allocated=ResourceSpec.from_dict(data["allocated"]),
            labels=data.get("labels", {}),
            health=NodeHealth(data.get("health", "healthy")),
            last_heartbeat_index=data.get("last_heartbeat_index", 0),
            running_tasks={TaskID.from_dict(t) for t in data.get("running_tasks", [])},
        )


@dataclass
class CommandResult:
    """Result of applying a command."""
    success: bool
    error: Optional[str] = None
    task: Optional[Task] = None
    fencing_token: Optional[int] = None


class ProcessTableState:
    """
    The state machine for the Decentralized OS.
    
    All modifications go through apply_command() for consistency.
    Uses log index for deterministic time - NO datetime.now() or time.time().
    
    Key invariants:
    1. Fencing tokens are monotonically increasing per task
    2. Leases expire at a specific log index (deterministic)
    3. Workers with outdated tokens cannot make state changes
    """
    
    # Default lease duration in log entries (configurable)
    DEFAULT_LEASE_DURATION = 1000  # ~1000 log entries = ~10 seconds at 100 writes/sec
    
    def __init__(self, shard_id: int = 0):
        self.shard_id = shard_id
        
        # Task storage
        self.tasks: Dict[TaskID, Task] = {}
        
        # Indexes for efficient queries
        self.tasks_by_status: Dict[TaskStatus, List[TaskID]] = defaultdict(list)
        self.tasks_by_namespace: Dict[str, List[TaskID]] = defaultdict(list)
        self.tasks_by_node: Dict[str, List[TaskID]] = defaultdict(list)
        
        # Node registry
        self.nodes: Dict[str, Node] = {}
        
        # ID generation
        self.next_task_sequence: int = 0
        self.next_fencing_token: int = 1  # Global fencing token counter
        
        # Current log index (deterministic "time")
        self.current_log_index: int = 0
        
        # Pending scheduling queue (priority queue)
        self.pending_queue: List[Tuple[int, TaskID]] = []
    
    # ========== Deterministic Time ==========
    
    def get_current_log_index(self) -> int:
        """Get the current log index (deterministic time)."""
        return self.current_log_index
    
    def advance_log_index(self) -> int:
        """
        Advance the log index by 1.
        Called after each command is applied.
        
        This is the ONLY way time advances in the state machine.
        """
        self.current_log_index += 1
        return self.current_log_index
    
    def set_log_index(self, index: int) -> None:
        """Set log index (used during recovery)."""
        self.current_log_index = index
    
    # ========== Fencing Tokens ==========
    
    def generate_fencing_token(self) -> int:
        """
        Generate a new fencing token.
        
        Fencing tokens are globally unique and monotonically increasing.
        They are used to prevent zombie workers from making stale updates.
        """
        token = self.next_fencing_token
        self.next_fencing_token += 1
        return token
    
    def validate_fencing_token(
        self, 
        task: Task, 
        provided_token: int
    ) -> Tuple[bool, str]:
        """
        Validate a fencing token for a task.
        
        Returns (is_valid, error_message).
        A token is valid if it matches the current token for the task.
        """
        if provided_token < task.fencing_token:
            return False, f"Stale fencing token: provided={provided_token}, current={task.fencing_token}"
        if provided_token > task.fencing_token:
            return False, f"Future fencing token: provided={provided_token}, current={task.fencing_token}"
        return True, ""
    
    # ========== Log-Based Leases ==========
    
    def is_lease_valid(self, task: Task, current_index: int) -> bool:
        """
        Check if a task's lease is still valid.
        
        A lease is valid if:
        1. current_log_index < lease_expiry_index
        2. The task is in RUNNING state
        """
        if task.status != TaskStatus.RUNNING:
            return False
        return current_index < task.lease_expiry_index
    
    def calculate_lease_expiry(
        self, 
        start_index: int, 
        duration: Optional[int] = None
    ) -> int:
        """
        Calculate lease expiry index.
        
        Args:
            start_index: Log index when lease starts
            duration: Lease duration in log entries (default: DEFAULT_LEASE_DURATION)
        
        Returns:
            Log index when lease expires
        """
        if duration is None:
            duration = self.DEFAULT_LEASE_DURATION
        return start_index + duration
    
    def check_expired_leases(self) -> List[Task]:
        """
        Check all running tasks for expired leases.
        
        Returns list of tasks with expired leases.
        Called periodically by the leader.
        """
        expired = []
        current_index = self.current_log_index
        
        for task in self.tasks.values():
            if task.status == TaskStatus.RUNNING:
                if current_index >= task.lease_expiry_index:
                    expired.append(task)
        
        return expired
    
    # ========== Command Application ==========
    
    def apply_command(
        self, 
        command_type: str, 
        command: dict,
        current_log_index: Optional[int] = None,
    ) -> CommandResult:
        """
        Apply a command to the state machine.
        
        This is the ONLY way to modify state.
        All commands are validated and applied atomically.
        
        Args:
            command_type: Type of command (e.g., "submit_task", "schedule_task")
            command: Command payload
            current_log_index: Current log index (for lease calculations)
        
        Returns:
            CommandResult with success/failure and any relevant data
        """
        if current_log_index is not None:
            self.current_log_index = current_log_index
        
        handlers = {
            "submit_task": self._apply_submit_task,
            "schedule_task": self._apply_schedule_task,
            "start_task": self._apply_start_task,
            "complete_task": self._apply_complete_task,
            "fail_task": self._apply_fail_task,
            "cancel_task": self._apply_cancel_task,
            "timeout_task": self._apply_timeout_task,
            "orphan_task": self._apply_orphan_task,
            "renew_lease": self._apply_renew_lease,
            "register_node": self._apply_register_node,
            "update_heartbeat": self._apply_update_heartbeat,
            "deregister_node": self._apply_deregister_node,
        }
        
        handler = handlers.get(command_type)
        if handler is None:
            return CommandResult(success=False, error=f"Unknown command type: {command_type}")
        
        return handler(command)
    
    # ========== Task Commands ==========
    
    def _apply_submit_task(self, cmd: dict) -> CommandResult:
        """Submit a new task."""
        # Generate task ID
        task_id = TaskID(
            shard=self.shard_id,
            sequence=self.next_task_sequence,
        )
        self.next_task_sequence += 1
        
        # Generate initial fencing token
        fencing_token = self.generate_fencing_token()
        
        # Create task
        task = Task(
            id=task_id,
            name=cmd["name"],
            namespace=cmd.get("namespace", "default"),
            image=cmd.get("image", ""),
            args=cmd.get("args", []),
            env=cmd.get("env", []),
            resources=ResourceSpec.from_dict(cmd.get("resources", {})),
            ttl_log_entries=cmd.get("ttl_log_entries", 0),
            max_retries=cmd.get("max_retries", 3),
            status=TaskStatus.PENDING,
            created_at_index=self.current_log_index,
            fencing_token=fencing_token,
            labels=cmd.get("labels", {}),
            submitted_by=cmd.get("submitted_by", ""),
        )
        
        # Store task
        self.tasks[task_id] = task
        
        # Update indexes
        self.tasks_by_status[TaskStatus.PENDING].append(task_id)
        self.tasks_by_namespace[task.namespace].append(task_id)
        
        # Add to scheduling queue
        priority = self._calculate_priority(task)
        import heapq
        heapq.heappush(self.pending_queue, (priority, task_id))
        
        return CommandResult(
            success=True, 
            task=task,
            fencing_token=fencing_token,
        )
    
    def _apply_schedule_task(self, cmd: dict) -> CommandResult:
        """Schedule a task to a worker node."""
        task_id = TaskID.from_dict(cmd["task_id"])
        node_id = NodeID.from_dict(cmd["node_id"])
        provided_token = cmd.get("fencing_token", 0)
        lease_duration = cmd.get("lease_duration", self.DEFAULT_LEASE_DURATION)
        
        task = self.tasks.get(task_id)
        if task is None:
            return CommandResult(success=False, error="Task not found")
        
        if task.status != TaskStatus.PENDING:
            return CommandResult(
                success=False, 
                error=f"Task is not pending (status={task.status.value})"
            )
        
        # Resource quota check (simple: max 100 tasks per node)
        current_on_node = len(self.tasks_by_node.get(node_id.id, []))
        if current_on_node >= 100:
            return CommandResult(success=False, error="Node quota exceeded (max 100 tasks)")
        
        # Generate new fencing token for this assignment
        # This invalidates any previous tokens (zombie worker protection)
        new_fencing_token = self.generate_fencing_token()
        
        # Update task
        old_status = task.status
        task.status = TaskStatus.SCHEDULED
        task.assigned_node = node_id
        task.scheduled_at_index = self.current_log_index
        task.fencing_token = new_fencing_token
        
        # Set lease expiry
        task.lease_expiry_index = self.calculate_lease_expiry(
            self.current_log_index, 
            lease_duration
        )
        
        # Update indexes
        self.tasks_by_status[old_status].remove(task_id)
        self.tasks_by_status[TaskStatus.SCHEDULED].append(task_id)
        self.tasks_by_node[node_id.id].append(task_id)
        
        # Remove from pending queue
        self.pending_queue = [
            (p, tid) for p, tid in self.pending_queue 
            if tid != task_id
        ]
        import heapq
        heapq.heapify(self.pending_queue)
        
        return CommandResult(
            success=True, 
            task=task,
            fencing_token=new_fencing_token,
        )
    
    def _apply_start_task(self, cmd: dict) -> CommandResult:
        """Mark a task as started (worker acknowledges assignment)."""
        task_id = TaskID.from_dict(cmd["task_id"])
        node_id = NodeID.from_dict(cmd["node_id"])
        provided_token = cmd.get("fencing_token", 0)
        
        task = self.tasks.get(task_id)
        if task is None:
            return CommandResult(success=False, error="Task not found")
        
        # Validate fencing token (ZOMBIE WORKER PROTECTION)
        is_valid, error = self.validate_fencing_token(task, provided_token)
        if not is_valid:
            return CommandResult(
                success=False, 
                error=f"Invalid fencing token: {error}"
            )
        
        # Validate node assignment
        if task.assigned_node is None or task.assigned_node.id != node_id.id:
            return CommandResult(
                success=False, 
                error=f"Task not assigned to node {node_id.id}"
            )
        
        if task.status != TaskStatus.SCHEDULED:
            return CommandResult(
                success=False, 
                error=f"Task is not scheduled (status={task.status.value})"
            )
        
        # Update task
        old_status = task.status
        task.status = TaskStatus.RUNNING
        task.started_at_index = self.current_log_index
        
        # Update indexes
        self.tasks_by_status[old_status].remove(task_id)
        self.tasks_by_status[TaskStatus.RUNNING].append(task_id)
        
        return CommandResult(success=True, task=task)
    
    def _apply_complete_task(self, cmd: dict) -> CommandResult:
        """Mark a task as completed successfully."""
        task_id = TaskID.from_dict(cmd["task_id"])
        node_id = NodeID.from_dict(cmd["node_id"])
        provided_token = cmd.get("fencing_token", 0)
        exit_code = cmd.get("exit_code", 0)
        
        task = self.tasks.get(task_id)
        if task is None:
            return CommandResult(success=False, error="Task not found")
        
        # Validate fencing token (ZOMBIE WORKER PROTECTION)
        is_valid, error = self.validate_fencing_token(task, provided_token)
        if not is_valid:
            return CommandResult(
                success=False, 
                error=f"Invalid fencing token: {error}"
            )
        
        # Validate node assignment
        if task.assigned_node is None or task.assigned_node.id != node_id.id:
            return CommandResult(
                success=False, 
                error=f"Task not assigned to node {node_id.id}"
            )
        
        if task.status != TaskStatus.RUNNING:
            return CommandResult(
                success=False, 
                error=f"Task is not running (status={task.status.value})"
            )
        
        # Update task
        old_status = task.status
        old_node = task.assigned_node
        task.status = TaskStatus.SUCCEEDED
        task.finished_at_index = self.current_log_index
        task.exit_code = exit_code
        
        # Update indexes
        self.tasks_by_status[old_status].remove(task_id)
        self.tasks_by_status[TaskStatus.SUCCEEDED].append(task_id)
        if old_node:
            self.tasks_by_node[old_node.id].remove(task_id)
        
        return CommandResult(success=True, task=task)
    
    def _apply_fail_task(self, cmd: dict) -> CommandResult:
        """Mark a task as failed."""
        task_id = TaskID.from_dict(cmd["task_id"])
        node_id = NodeID.from_dict(cmd["node_id"])
        provided_token = cmd.get("fencing_token", 0)
        error_message = cmd.get("error_message", "")
        should_retry = cmd.get("should_retry", True)
        
        task = self.tasks.get(task_id)
        if task is None:
            return CommandResult(success=False, error="Task not found")
        
        # Validate fencing token (ZOMBIE WORKER PROTECTION)
        is_valid, error = self.validate_fencing_token(task, provided_token)
        if not is_valid:
            return CommandResult(
                success=False, 
                error=f"Invalid fencing token: {error}"
            )
        
        # Validate node assignment
        if task.assigned_node is None or task.assigned_node.id != node_id.id:
            return CommandResult(
                success=False, 
                error=f"Task not assigned to node {node_id.id}"
            )
        
        if task.status != TaskStatus.RUNNING:
            return CommandResult(
                success=False, 
                error=f"Task is not running (status={task.status.value})"
            )
        
        old_status = task.status
        old_node = task.assigned_node
        
        # Check if we should retry
        if should_retry and task.retry_count < task.max_retries:
            # Reschedule the task
            task.status = TaskStatus.PENDING
            task.retry_count += 1
            task.assigned_node = None
            task.error_message = error_message
            
            # Generate new fencing token
            task.fencing_token = self.generate_fencing_token()
            
            # Add back to pending queue
            import heapq
            priority = self._calculate_priority(task)
            heapq.heappush(self.pending_queue, (priority, task_id))
            
            # Update indexes
            self.tasks_by_status[old_status].remove(task_id)
            self.tasks_by_status[TaskStatus.PENDING].append(task_id)
            if old_node:
                self.tasks_by_node[old_node.id].remove(task_id)
        else:
            # Mark as failed
            task.status = TaskStatus.FAILED
            task.finished_at_index = self.current_log_index
            task.error_message = error_message
            
            # Update indexes
            self.tasks_by_status[old_status].remove(task_id)
            self.tasks_by_status[TaskStatus.FAILED].append(task_id)
            if old_node:
                self.tasks_by_node[old_node.id].remove(task_id)
        
        return CommandResult(success=True, task=task)
    
    def _apply_cancel_task(self, cmd: dict) -> CommandResult:
        """Cancel a task."""
        task_id = TaskID.from_dict(cmd["task_id"])
        reason = cmd.get("reason", "")
        
        task = self.tasks.get(task_id)
        if task is None:
            return CommandResult(success=False, error="Task not found")
        
        # Can only cancel non-terminal tasks
        terminal_states = {
            TaskStatus.SUCCEEDED, 
            TaskStatus.FAILED, 
            TaskStatus.CANCELLED,
            TaskStatus.TIMEOUT,
        }
        if task.status in terminal_states:
            return CommandResult(
                success=False, 
                error=f"Task is already in terminal state: {task.status.value}"
            )
        
        # Generate new fencing token to invalidate any running workers
        new_fencing_token = self.generate_fencing_token()
        
        old_status = task.status
        old_node = task.assigned_node
        
        task.status = TaskStatus.CANCELLED
        task.finished_at_index = self.current_log_index
        task.error_message = f"Cancelled: {reason}"
        task.fencing_token = new_fencing_token
        
        # Update indexes
        self.tasks_by_status[old_status].remove(task_id)
        self.tasks_by_status[TaskStatus.CANCELLED].append(task_id)
        if old_node:
            self.tasks_by_node[old_node.id].remove(task_id)
        
        # Remove from pending queue if there
        self.pending_queue = [
            (p, tid) for p, tid in self.pending_queue 
            if tid != task_id
        ]
        import heapq
        heapq.heapify(self.pending_queue)
        
        return CommandResult(
            success=True, 
            task=task,
            fencing_token=new_fencing_token,
        )
    
    def _apply_timeout_task(self, cmd: dict) -> CommandResult:
        """Mark a task as timed out (deterministic, based on log index)."""
        task_id = TaskID.from_dict(cmd["task_id"])
        
        task = self.tasks.get(task_id)
        if task is None:
            return CommandResult(success=False, error="Task not found")
        
        if task.status != TaskStatus.RUNNING:
            return CommandResult(
                success=False, 
                error=f"Task is not running (status={task.status.value})"
            )
        
        # Check TTL
        if task.ttl_log_entries > 0:
            elapsed = self.current_log_index - task.started_at_index
            if elapsed < task.ttl_log_entries:
                return CommandResult(
                    success=False, 
                    error=f"TTL not expired: elapsed={elapsed}, ttl={task.ttl_log_entries}"
                )
        
        old_status = task.status
        old_node = task.assigned_node
        
        # Generate new fencing token
        new_fencing_token = self.generate_fencing_token()
        
        task.status = TaskStatus.TIMEOUT
        task.finished_at_index = self.current_log_index
        task.error_message = "Task exceeded TTL"
        task.fencing_token = new_fencing_token
        
        # Update indexes
        self.tasks_by_status[old_status].remove(task_id)
        self.tasks_by_status[TaskStatus.TIMEOUT].append(task_id)
        if old_node:
            self.tasks_by_node[old_node.id].remove(task_id)
        
        return CommandResult(
            success=True, 
            task=task,
            fencing_token=new_fencing_token,
        )
    
    def _apply_orphan_task(self, cmd: dict) -> CommandResult:
        """Mark a task as orphaned (worker died)."""
        task_id = TaskID.from_dict(cmd["task_id"])
        dead_node = NodeID.from_dict(cmd.get("dead_node", {}))
        
        task = self.tasks.get(task_id)
        if task is None:
            return CommandResult(success=False, error="Task not found")
        
        if task.status not in {TaskStatus.SCHEDULED, TaskStatus.RUNNING}:
            return CommandResult(
                success=False, 
                error=f"Task cannot be orphaned (status={task.status.value})"
            )
        
        old_status = task.status
        old_node = task.assigned_node
        
        # Generate new fencing token
        new_fencing_token = self.generate_fencing_token()
        
        # Reschedule the task
        task.status = TaskStatus.PENDING
        task.assigned_node = None
        task.error_message = f"Orphaned by dead node: {dead_node.id}"
        task.fencing_token = new_fencing_token
        
        # Add back to pending queue
        import heapq
        priority = self._calculate_priority(task)
        heapq.heappush(self.pending_queue, (priority, task_id))
        
        # Update indexes
        self.tasks_by_status[old_status].remove(task_id)
        self.tasks_by_status[TaskStatus.PENDING].append(task_id)
        if old_node:
            self.tasks_by_node[old_node.id].remove(task_id)
        
        return CommandResult(
            success=True, 
            task=task,
            fencing_token=new_fencing_token,
        )
    
    def _apply_renew_lease(self, cmd: dict) -> CommandResult:
        """Renew a task's lease."""
        task_id = TaskID.from_dict(cmd["task_id"])
        node_id = NodeID.from_dict(cmd["node_id"])
        provided_token = cmd.get("fencing_token", 0)
        additional_entries = cmd.get("additional_entries", self.DEFAULT_LEASE_DURATION)
        
        task = self.tasks.get(task_id)
        if task is None:
            return CommandResult(success=False, error="Task not found")
        
        # Validate fencing token (ZOMBIE WORKER PROTECTION)
        is_valid, error = self.validate_fencing_token(task, provided_token)
        if not is_valid:
            return CommandResult(
                success=False, 
                error=f"Invalid fencing token: {error}"
            )
        
        if task.status != TaskStatus.RUNNING:
            return CommandResult(
                success=False, 
                error=f"Task is not running (status={task.status.value})"
            )
        
        # Extend lease
        task.lease_expiry_index = self.current_log_index + additional_entries
        
        return CommandResult(success=True, task=task)
    
    # ========== Node Commands ==========
    
    def _apply_register_node(self, cmd: dict) -> CommandResult:
        """Register a new worker node."""
        node_id = NodeID.from_dict(cmd["node_id"])
        
        if node_id.id in self.nodes:
            return CommandResult(success=False, error="Node already registered")
        
        node = Node(
            id=node_id,
            hostname=cmd.get("hostname", ""),
            capacity=ResourceSpec.from_dict(cmd.get("capacity", {})),
            allocated=ResourceSpec(),
            labels=cmd.get("labels", {}),
            health=NodeHealth.HEALTHY,
            last_heartbeat_index=self.current_log_index,
            running_tasks=set(),
        )
        
        self.nodes[node_id.id] = node
        
        return CommandResult(success=True)
    
    def _apply_update_heartbeat(self, cmd: dict) -> CommandResult:
        """Update node heartbeat."""
        node_id = NodeID.from_dict(cmd["node_id"])
        
        node = self.nodes.get(node_id.id)
        if node is None:
            return CommandResult(success=False, error="Node not registered")
        
        node.last_heartbeat_index = self.current_log_index
        node.health = NodeHealth(cmd.get("health", "healthy"))
        node.allocated = ResourceSpec.from_dict(cmd.get("allocated", {}))
        node.running_tasks = {
            TaskID.from_dict(t) for t in cmd.get("running_tasks", [])
        }
        
        return CommandResult(success=True)
    
    def _apply_deregister_node(self, cmd: dict) -> CommandResult:
        """Deregister a worker node."""
        node_id = NodeID.from_dict(cmd["node_id"])
        
        node = self.nodes.get(node_id.id)
        if node is None:
            return CommandResult(success=False, error="Node not registered")
        
        # Orphan all running tasks on this node
        orphaned_tasks = []
        for task_id in node.running_tasks:
            task = self.tasks.get(task_id)
            if task and task.status in {TaskStatus.SCHEDULED, TaskStatus.RUNNING}:
                # Generate new fencing token
                new_fencing_token = self.generate_fencing_token()
                
                old_status = task.status
                task.status = TaskStatus.PENDING
                task.assigned_node = None
                task.error_message = f"Orphaned by deregistered node: {node_id.id}"
                task.fencing_token = new_fencing_token
                
                # Update indexes
                self.tasks_by_status[old_status].remove(task_id)
                self.tasks_by_status[TaskStatus.PENDING].append(task_id)
                
                # Add back to pending queue
                import heapq
                priority = self._calculate_priority(task)
                heapq.heappush(self.pending_queue, (priority, task_id))
                
                orphaned_tasks.append(task_id)
        
        # Remove node
        del self.nodes[node_id.id]
        
        # Remove from node index
        if node_id.id in self.tasks_by_node:
            del self.tasks_by_node[node_id.id]
        
        return CommandResult(success=True)
    
    # ========== Utility Methods ==========
    
    def _calculate_priority(self, task: Task) -> int:
        """Calculate scheduling priority for a task (lower = higher priority)."""
        # Base priority on creation time (FIFO)
        priority = task.created_at_index
        
        # Higher priority for tasks with more retries (starvation prevention)
        priority -= task.retry_count * 1000
        
        return priority
    
    def get_pending_tasks(self, limit: int) -> List[Task]:
        """Get pending tasks for scheduling."""
        import heapq
        
        result = []
        temp_queue = list(self.pending_queue)
        heapq.heapify(temp_queue)
        
        while temp_queue and len(result) < limit:
            priority, task_id = heapq.heappop(temp_queue)
            task = self.tasks.get(task_id)
            if task and task.status == TaskStatus.PENDING:
                result.append(task)
        
        return result
    
    def get_task(self, task_id: TaskID) -> Optional[Task]:
        """Get a task by ID."""
        return self.tasks.get(task_id)
    
    def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
        namespace: Optional[str] = None,
        limit: int = 100,
    ) -> List[Task]:
        """List tasks with optional filtering."""
        if status is not None:
            task_ids = self.tasks_by_status.get(status, [])
        elif namespace is not None:
            task_ids = self.tasks_by_namespace.get(namespace, [])
        else:
            task_ids = list(self.tasks.keys())
        
        tasks = [self.tasks[tid] for tid in task_ids if tid in self.tasks]
        return tasks[:limit]
    
    def check_node_failures(self, timeout_entries: int = 5000) -> List[Node]:
        """
        Check for node failures based on heartbeat timeout.
        
        Args:
            timeout_entries: Number of log entries without heartbeat to consider dead
        
        Returns:
            List of nodes that have failed
        """
        failed = []
        threshold = self.current_log_index - timeout_entries
        
        for node in self.nodes.values():
            if node.last_heartbeat_index < threshold:
                failed.append(node)
        
        return failed
    
    # ========== Serialization ==========
    
    def to_dict(self) -> dict:
        """Serialize state to dictionary."""
        return {
            "shard_id": self.shard_id,
            "tasks": {str(tid): task.to_dict() for tid, task in self.tasks.items()},
            "nodes": {nid: node.to_dict() for nid, node in self.nodes.items()},
            "next_task_sequence": self.next_task_sequence,
            "next_fencing_token": self.next_fencing_token,
            "current_log_index": self.current_log_index,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "ProcessTableState":
        """Deserialize state from dictionary."""
        state = cls(shard_id=data.get("shard_id", 0))
        
        state.next_task_sequence = data.get("next_task_sequence", 0)
        state.next_fencing_token = data.get("next_fencing_token", 1)
        state.current_log_index = data.get("current_log_index", 0)
        
        # Restore tasks
        for tid_str, task_data in data.get("tasks", {}).items():
            task = Task.from_dict(task_data)
            state.tasks[task.id] = task
            
            # Rebuild indexes
            state.tasks_by_status[task.status].append(task.id)
            state.tasks_by_namespace[task.namespace].append(task.id)
            if task.assigned_node:
                state.tasks_by_node[task.assigned_node.id].append(task.id)
        
        # Restore nodes
        for nid, node_data in data.get("nodes", {}).items():
            state.nodes[nid] = Node.from_dict(node_data)
        
        return state
    
    def to_snapshot(self) -> bytes:
        """Serialize state for snapshot (compressed)."""
        data = json.dumps(self.to_dict(), separators=(',', ':')).encode('utf-8')
        return zlib.compress(data, level=6)
    
    @classmethod
    def from_snapshot(cls, data: bytes) -> "ProcessTableState":
        """Deserialize state from snapshot."""
        decompressed = zlib.decompress(data)
        return cls.from_dict(json.loads(decompressed.decode('utf-8')))
