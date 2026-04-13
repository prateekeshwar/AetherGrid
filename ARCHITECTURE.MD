# AetherGrid: Decentralized OS & Task Orchestration Platform

## Architectural Blueprint v1.0

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Core Components](#3-core-components)
4. [Data Models](#4-data-models)
5. [Storage Layer Redesign](#5-storage-layer-redesign)
6. [Network Layer](#6-network-layer)
7. [Security Model](#7-security-model)
8. [Scalability Strategy](#8-scalability-strategy)
9. [Observability & Operations](#9-observability--operations)
10. [Implementation Roadmap](#10-implementation-roadmap)

---

## 1. Executive Summary

### Vision

Transform AetherGrid from a prototype Raft-based KV store into a **Decentralized Operating System Kernel** that manages:

- **Global Process Table**: Distributed, strongly-consistent registry of all tasks
- **Resource Allocation**: CPU, memory, GPU scheduling across cluster nodes
- **Task Lifecycle Management**: Submit → Schedule → Execute → Monitor → Complete
- **Fault Tolerance**: Automatic task rescheduling on node failures

### Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Multi-Raft Sharding | Horizontal scaling beyond single-leader limits |
| LSM-Tree Storage (RocksDB) | O(1) writes, efficient range scans for task queries |
| gRPC + Protobuf | Type-safe, efficient serialization, streaming support |
| Read-Replica Pattern | Followers serve task queries, leader handles writes |
| Hybrid Consistency | Strong for writes, eventual for metrics/logs |

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            CLIENT LAYER                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │     CLI      │  │  SDK (Python)│  │  SDK (Go)    │  │  Web UI      │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │ gRPC / REST
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         API GATEWAY LAYER                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  - Authentication (mTLS, JWT)                                       │   │
│  │  - Rate Limiting & Quotas                                           │   │
│  │  - Request Routing (hash(task_id) → Shard)                          │   │
│  │  - Response Caching                                                 │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      CONSENSUS LAYER (Multi-Raft)                            │
│                                                                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │   Shard 0       │  │   Shard 1       │  │   Shard N       │             │
│  │  (Tasks 0-1M)   │  │  (Tasks 1M-2M)  │  │  (Tasks N-M)    │             │
│  │                 │  │                 │  │                 │             │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │             │
│  │  │  Leader   │  │  │  │  Leader   │  │  │  │  Leader   │  │             │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │             │
│  │  ┌─────┬─────┐  │  │  ┌─────┬─────┐  │  │  ┌─────┬─────┐  │             │
│  │  │ F1  │ F2  │  │  │  │ F1  │ F2  │  │  │  │ F1  │ F2  │  │             │
│  │  └─────┴─────┘  │  │  └─────┴─────┘  │  │  └─────┴─────┘  │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                    META RAFT (Cluster Config)                        │   │
│  │  - Node membership                                                  │   │
│  │  - Shard assignments                                                │   │
│  │  - Cluster-wide configuration                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STORAGE LAYER                                        │
│                                                                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │   RocksDB       │  │   RocksDB       │  │   RocksDB       │             │
│  │  (Node 1)       │  │  (Node 2)       │  │  (Node 3)       │             │
│  │                 │  │                 │  │                 │             │
│  │  - WAL          │  │  - WAL          │  │  - WAL          │             │
│  │  - SSTables     │  │  - SSTables     │  │  - SSTables     │             │
│  │  - MemTable     │  │  - MemTable     │  │  - MemTable     │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXECUTION LAYER                                      │
│                                                                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │   Worker Node   │  │   Worker Node   │  │   Worker Node   │             │
│  │                 │  │                 │  │                 │             │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │             │
│  │  │  Runtime  │  │  │  │  Runtime  │  │  │  │  Runtime  │  │             │
│  │  │ (Docker)  │  │  │  │ (gVisor)  │  │  │  │  (WASM)   │  │             │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │             │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │             │
│  │  │  Agent    │  │  │  │  Agent    │  │  │  │  Agent    │  │             │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │             │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Core Components

### 3.1 Process Table Manager

The heart of the Decentralized OS - manages the global process table.

```python
# Conceptual Interface
class ProcessTableManager:
    """Manages the global distributed process table."""
    
    async def submit_task(
        self,
        name: str,
        image: str,           # Container image / WASM module
        args: List[str],
        resources: ResourceSpec,
        constraints: PlacementConstraints,
        ttl: Optional[timedelta],
    ) -> TaskID:
        """Submit a new task to the cluster."""
        pass
    
    async def get_task(self, task_id: TaskID) -> Optional[Task]:
        """Get task metadata and current state."""
        pass
    
    async def list_tasks(
        self,
        filter: TaskFilter,
        page_token: Optional[str],
        page_size: int,
    ) -> ListTasksResponse:
        """List tasks with filtering and pagination."""
        pass
    
    async def cancel_task(self, task_id: TaskID, force: bool) -> CancelResult:
        """Cancel a running or pending task."""
        pass
    
    async def get_task_logs(
        self,
        task_id: TaskID,
        follow: bool,
        tail_lines: Optional[int],
    ) -> AsyncIterator[LogEntry]:
        """Stream task logs."""
        pass
```

### 3.2 Scheduler

Distributed task scheduler with pluggable strategies.

```python
class Scheduler:
    """Distributed task scheduler."""
    
    def __init__(self, strategy: SchedulingStrategy):
        self.strategy = strategy
        self.resource_tracker = ResourceTracker()
    
    async def schedule(self, task: Task) -> Assignment:
        """
        Assign task to a worker node.
        
        Called by the Raft leader after task is committed.
        """
        # Get current cluster state
        cluster_view = await self.resource_tracker.get_cluster_view()
        
        # Find candidate nodes
        candidates = self.strategy.filter_candidates(task, cluster_view)
        
        # Score and rank
        scores = self.strategy.score(task, candidates)
        
        # Select best node
        selected = self.strategy.select(scores)
        
        return Assignment(
            task_id=task.id,
            node_id=selected.node_id,
            resources_allocated=selected.resources,
        )
```

### 3.3 Worker Agent

Runs on each worker node, executes tasks and reports status.

```python
class WorkerAgent:
    """Agent that runs on worker nodes."""
    
    def __init__(self, node_id: NodeID, runtime: TaskRuntime):
        self.node_id = node_id
        self.runtime = runtime  # Docker, gVisor, WASM
        self.running_tasks: Dict[TaskID, TaskHandle] = {}
    
    async def execute_task(self, assignment: Assignment) -> None:
        """Execute an assigned task."""
        task = assignment.task
        
        # Pull image / module
        await self.runtime.prepare(task.image, task.args)
        
        # Create isolation boundary
        container = await self.runtime.create_container(
            task_id=task.id,
            image=task.image,
            args=task.args,
            resources=assignment.resources_allocated,
            env=task.env,
        )
        
        # Start execution
        handle = await container.start()
        self.running_tasks[task.id] = handle
        
        # Stream logs to consensus layer
        asyncio.create_task(self._stream_logs(task.id, handle))
        
        # Monitor and report status
        asyncio.create_task(self._monitor_task(task.id, handle))
    
    async def _monitor_task(self, task_id: TaskID, handle: TaskHandle) -> None:
        """Monitor task execution and report status changes."""
        exit_code = await handle.wait()
        
        status = TaskStatus.COMPLETED if exit_code == 0 else TaskStatus.FAILED
        
        # Report to leader via Raft
        await self.report_status(
            task_id=task_id,
            status=status,
            exit_code=exit_code,
            finished_at=datetime.utcnow(),
        )
```

### 3.4 Resource Tracker

Tracks cluster resource utilization.

```python
class ResourceTracker:
    """Tracks resource utilization across the cluster."""
    
    def __init__(self):
        # Stored in Raft state machine for consistency
        self.node_resources: Dict[NodeID, NodeResources] = {}
        self.allocations: Dict[TaskID, ResourceAllocation] = {}
    
    async def update_node_heartbeat(
        self,
        node_id: NodeID,
        available: ResourceSpec,
        running_tasks: List[TaskID],
    ) -> None:
        """Update node resource snapshot (called via heartbeat)."""
        self.node_resources[node_id] = NodeResources(
            node_id=node_id,
            total=available,
            allocated=self._calculate_allocated(running_tasks),
            last_heartbeat=datetime.utcnow(),
        )
    
    async def allocate(
        self,
        task_id: TaskID,
        node_id: NodeID,
        resources: ResourceSpec,
    ) -> bool:
        """Allocate resources for a task (atomic via Raft)."""
        node = self.node_resources.get(node_id)
        if not node or not node.can_allocate(resources):
            return False
        
        node.allocated += resources
        self.allocations[task_id] = ResourceAllocation(
            task_id=task_id,
            node_id=node_id,
            resources=resources,
        )
        return True
    
    async def release(self, task_id: TaskID) -> None:
        """Release resources when task completes."""
        allocation = self.allocations.pop(task_id, None)
        if allocation:
            node = self.node_resources.get(allocation.node_id)
            if node:
                node.allocated -= allocation.resources
```

---

## 4. Data Models

### 4.1 Protobuf Schema

```protobuf
// aethergrid.proto

syntax = "proto3";

package aethergrid;

import "google/protobuf/timestamp.proto";
import "google/protobuf/duration.proto";

// ============== Core Types ==============

enum TaskStatus {
    TASK_STATUS_UNSPECIFIED = 0;
    TASK_STATUS_PENDING = 1;        // Submitted, waiting for scheduling
    TASK_STATUS_SCHEDULED = 2;      // Assigned to a node, not yet running
    TASK_STATUS_RUNNING = 3;        // Currently executing
    TASK_STATUS_SUCCEEDED = 4;      // Completed successfully
    TASK_STATUS_FAILED = 5;         // Failed with error
    TASK_STATUS_CANCELLED = 6;      // Cancelled by user
    TASK_STATUS_TIMEOUT = 7;        // Exceeded TTL
}

message TaskID {
    uint64 shard = 1;      // Which shard owns this task
    uint64 sequence = 2;   // Monotonic sequence within shard
}

message NodeID {
    string id = 1;  // UUID or hostname
}

message ResourceSpec {
    int64 cpu_millis = 1;      // CPU in milli-cores (1000 = 1 CPU)
    int64 memory_bytes = 2;    // Memory in bytes
    int64 gpu_count = 3;       // Number of GPUs
    map<string, string> extended = 4;  // Extended resources (e.g., "nvidia.com/gpu-memory")
}

message PlacementConstraints {
    repeated string required_labels = 1;    // Node must have these labels
    repeated string preferred_labels = 2;   // Prefer nodes with these labels
    repeated NodeID anti_affinity = 3;      // Avoid these nodes
    string zone = 4;                        // Availability zone constraint
}

// ============== Task ==============

message Task {
    TaskID id = 1;
    string name = 2;
    string namespace = 3;
    
    // Spec
    string image = 10;                       // Container image or WASM module
    repeated string args = 11;               // Command arguments
    repeated string env = 12;                // Environment variables (KEY=VALUE)
    ResourceSpec resources = 13;             // Resource requirements
    PlacementConstraints constraints = 14;   // Placement constraints
    google.protobuf.Duration ttl = 15;       // Time-to-live
    int32 max_retries = 16;                  // Max retry attempts
    
    // Status
    TaskStatus status = 20;
    NodeID assigned_node = 21;
    google.protobuf.Timestamp created_at = 22;
    google.protobuf.Timestamp scheduled_at = 23;
    google.protobuf.Timestamp started_at = 24;
    google.protobuf.Timestamp finished_at = 25;
    int32 exit_code = 26;
    string error_message = 27;
    int32 retry_count = 28;
    
    // Metadata
    map<string, string> labels = 30;
    map<string, string> annotations = 31;
    string submitted_by = 32;  // User/service that submitted
}

// ============== Commands (State Machine Inputs) ==============

message Command {
    oneof command {
        SubmitTaskCommand submit_task = 1;
        UpdateTaskStatusCommand update_status = 2;
        CancelTaskCommand cancel_task = 3;
        RegisterNodeCommand register_node = 4;
        UpdateNodeHeartbeatCommand update_heartbeat = 5;
        DeregisterNodeCommand deregister_node = 6;
    }
}

message SubmitTaskCommand {
    string name = 1;
    string namespace = 2;
    string image = 3;
    repeated string args = 4;
    repeated string env = 5;
    ResourceSpec resources = 6;
    PlacementConstraints constraints = 7;
    google.protobuf.Duration ttl = 8;
    int32 max_retries = 9;
    map<string, string> labels = 10;
    string submitted_by = 11;
}

message UpdateTaskStatusCommand {
    TaskID task_id = 1;
    TaskStatus new_status = 2;
    NodeID assigned_node = 3;
    int32 exit_code = 4;
    string error_message = 5;
}

message CancelTaskCommand {
    TaskID task_id = 1;
    bool force = 2;
    string reason = 3;
}

message RegisterNodeCommand {
    NodeID node_id = 1;
    string hostname = 2;
    ResourceSpec capacity = 3;
    map<string, string> labels = 4;
    repeated string available_runtimes = 5;  // ["docker", "wasm", "gvisor"]
}

message UpdateNodeHeartbeatCommand {
    NodeID node_id = 1;
    ResourceSpec available = 2;
    repeated TaskID running_tasks = 3;
    NodeHealth health = 4;
}

message DeregisterNodeCommand {
    NodeID node_id = 1;
    string reason = 2;
}

// ============== Node Health ==============

enum NodeHealth {
    NODE_HEALTH_UNSPECIFIED = 0;
    NODE_HEALTH_HEALTHY = 1;
    NODE_HEALTH_DEGRADED = 2;    // Some issues but functional
    NODE_HEALTH_UNHEALTHY = 3;   // Should not receive new tasks
}

message Node {
    NodeID id = 1;
    string hostname = 2;
    ResourceSpec capacity = 3;
    ResourceSpec allocated = 4;
    map<string, string> labels = 5;
    NodeHealth health = 6;
    google.protobuf.Timestamp last_heartbeat = 7;
    repeated string available_runtimes = 8;
    repeated TaskID running_tasks = 9;
}

// ============== RPC Messages ==============

message SubmitTaskRequest {
    SubmitTaskCommand spec = 1;
}

message SubmitTaskResponse {
    Task task = 1;
}

message GetTaskRequest {
    TaskID task_id = 1;
}

message GetTaskResponse {
    Task task = 1;
}

message ListTasksRequest {
    string namespace = 1;
    TaskStatus status_filter = 2;
    string label_selector = 3;
    int32 page_size = 4;
    string page_token = 5;
}

message ListTasksResponse {
    repeated Task tasks = 1;
    string next_page_token = 2;
}

// ============== Raft Messages ==============

message AppendEntriesRequest {
    uint64 term = 1;
    uint64 leader_id = 2;
    uint64 prev_log_index = 3;
    uint64 prev_log_term = 4;
    repeated LogEntry entries = 5;
    uint64 leader_commit = 6;
}

message LogEntry {
    uint64 term = 1;
    uint64 index = 2;
    Command command = 3;
}

message AppendEntriesResponse {
    uint64 term = 1;
    bool success = 2;
    uint64 match_index = 3;
    uint64 conflict_index = 4;
}

message RequestVoteRequest {
    uint64 term = 1;
    uint64 candidate_id = 2;
    uint64 last_log_index = 3;
    uint64 last_log_term = 4;
}

message RequestVoteResponse {
    uint64 term = 1;
    bool vote_granted = 2;
}

message InstallSnapshotRequest {
    uint64 term = 1;
    uint64 leader_id = 2;
    uint64 last_included_index = 3;
    uint64 last_included_term = 4;
    bytes snapshot_data = 5;
    uint64 offset = 6;
    bool done = 7;
}

// ============== Snapshot ==============

message Snapshot {
    uint64 last_included_index = 1;
    uint64 last_included_term = 2;
    
    // State machine snapshot
    ProcessTableSnapshot process_table = 10;
    ClusterStateSnapshot cluster_state = 11;
}

message ProcessTableSnapshot {
    repeated Task tasks = 1;
    uint64 next_task_id = 2;
}

message ClusterStateSnapshot {
    repeated Node nodes = 1;
    map<string, string> config = 2;
}
```

### 4.2 State Machine Design

```python
# state_machine.py

from dataclasses import dataclass, field
from typing import Dict, List, Optional
from collections import defaultdict
import heapq

@dataclass
class ProcessTableState:
    """
    The state machine for the Decentralized OS.
    
    All modifications go through apply_command() for consistency.
    """
    
    # Task storage (indexed by task_id)
    tasks: Dict[TaskID, Task] = field(default_factory=dict)
    
    # Indexes for efficient queries
    tasks_by_status: Dict[TaskStatus, List[TaskID]] = field(
        default_factory=lambda: defaultdict(list)
    )
    tasks_by_namespace: Dict[str, List[TaskID]] = field(
        default_factory=lambda: defaultdict(list)
    )
    tasks_by_node: Dict[NodeID, List[TaskID]] = field(
        default_factory=lambda: defaultdict(list)
    )
    
    # Node registry
    nodes: Dict[NodeID, Node] = field(default_factory=dict)
    
    # ID generation
    next_task_sequence: int = 0
    
    # Pending scheduling queue (priority queue)
    pending_queue: List[Tuple[int, TaskID]] = field(default_factory=list)
    
    def apply_command(self, command: Command) -> Result:
        """
        Apply a command to the state machine.
        
        This is called by the Raft leader after committing to the log.
        All state mutations go through this method.
        """
        if command.HasField('submit_task'):
            return self._apply_submit_task(command.submit_task)
        elif command.HasField('update_status'):
            return self._apply_update_status(command.update_status)
        elif command.HasField('cancel_task'):
            return self._apply_cancel_task(command.cancel_task)
        elif command.HasField('register_node'):
            return self._apply_register_node(command.register_node)
        elif command.HasField('update_heartbeat'):
            return self._apply_update_heartbeat(command.update_heartbeat)
        elif command.HasField('deregister_node'):
            return self._apply_deregister_node(command.deregister_node)
    
    def _apply_submit_task(self, cmd: SubmitTaskCommand) -> Task:
        """Create and store a new task."""
        # Generate task ID
        task_id = TaskID(
            shard=self.shard_id,
            sequence=self.next_task_sequence,
        )
        self.next_task_sequence += 1
        
        # Create task
        task = Task(
            id=task_id,
            name=cmd.name,
            namespace=cmd.namespace,
            image=cmd.image,
            args=cmd.args,
            env=cmd.env,
            resources=cmd.resources,
            constraints=cmd.constraints,
            ttl=cmd.ttl,
            max_retries=cmd.max_retries,
            labels=cmd.labels,
            submitted_by=cmd.submitted_by,
            status=TaskStatus.PENDING,
            created_at=datetime.utcnow(),
        )
        
        # Store task
        self.tasks[task_id] = task
        
        # Update indexes
        self.tasks_by_status[TaskStatus.PENDING].append(task_id)
        self.tasks_by_namespace[cmd.namespace].append(task_id)
        
        # Add to scheduling queue (lower priority = scheduled first)
        priority = self._calculate_priority(task)
        heapq.heappush(self.pending_queue, (priority, task_id))
        
        return task
    
    def _apply_update_status(self, cmd: UpdateTaskStatusCommand) -> Optional[Task]:
        """Update task status."""
        task = self.tasks.get(cmd.task_id)
        if not task:
            return None
        
        old_status = task.status
        new_status = cmd.new_status
        
        # Update indexes
        self.tasks_by_status[old_status].remove(cmd.task_id)
        self.tasks_by_status[new_status].append(cmd.task_id)
        
        # Handle node assignment
        if cmd.assigned_node:
            if task.assigned_node:
                self.tasks_by_node[task.assigned_node].remove(cmd.task_id)
            task.assigned_node = cmd.assigned_node
            self.tasks_by_node[cmd.assigned_node].append(cmd.task_id)
        
        # Update task
        task.status = new_status
        task.exit_code = cmd.exit_code
        task.error_message = cmd.error_message
        
        if new_status == TaskStatus.RUNNING:
            task.started_at = datetime.utcnow()
        elif new_status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED, 
                           TaskStatus.CANCELLED, TaskStatus.TIMEOUT):
            task.finished_at = datetime.utcnow()
        
        return task
    
    def _apply_cancel_task(self, cmd: CancelTaskCommand) -> Optional[Task]:
        """Cancel a task."""
        task = self.tasks.get(cmd.task_id)
        if not task:
            return None
        
        if task.status in (TaskStatus.SUCCEEDED, TaskStatus.FAILED, 
                          TaskStatus.CANCELLED):
            return task  # Already terminal
        
        # Update status
        old_status = task.status
        task.status = TaskStatus.CANCELLED
        task.finished_at = datetime.utcnow()
        
        # Update indexes
        self.tasks_by_status[old_status].remove(cmd.task_id)
        self.tasks_by_status[TaskStatus.CANCELLED].append(cmd.task_id)
        
        # Remove from pending queue if there
        if old_status == TaskStatus.PENDING:
            self.pending_queue = [
                (p, tid) for p, tid in self.pending_queue 
                if tid != cmd.task_id
            ]
            heapq.heapify(self.pending_queue)
        
        return task
    
    def get_pending_tasks(self, limit: int) -> List[Task]:
        """Get pending tasks for scheduling."""
        result = []
        while self.pending_queue and len(result) < limit:
            priority, task_id = heapq.heappop(self.pending_queue)
            task = self.tasks.get(task_id)
            if task and task.status == TaskStatus.PENDING:
                result.append(task)
        return result
    
    def to_snapshot(self) -> bytes:
        """Serialize state for snapshot."""
        snapshot = Snapshot(
            last_included_index=self.last_applied,
            last_included_term=self.current_term,
            process_table=ProcessTableSnapshot(
                tasks=list(self.tasks.values()),
                next_task_id=self.next_task_sequence,
            ),
            cluster_state=ClusterStateSnapshot(
                nodes=list(self.nodes.values()),
            ),
        )
        return snapshot.SerializeToString()
    
    @classmethod
    def from_snapshot(cls, data: bytes) -> 'ProcessTableState':
        """Deserialize state from snapshot."""
        snapshot = Snapshot()
        snapshot.ParseFromString(data)
        
        state = cls()
        for task in snapshot.process_table.tasks:
            state.tasks[task.id] = task
            state.tasks_by_status[task.status].append(task.id)
            state.tasks_by_namespace[task.namespace].append(task.id)
            if task.assigned_node:
                state.tasks_by_node[task.assigned_node].append(task.id)
        
        state.next_task_sequence = snapshot.process_table.next_task_id
        
        for node in snapshot.cluster_state.nodes:
            state.nodes[node.id] = node
        
        return state
```

---

## 5. Storage Layer Redesign

### 5.1 Current Problems

| Issue | Impact |
|-------|--------|
| Full JSON file rewrites | O(n) writes, blocks event loop |
| No compaction | Log grows unbounded |
| No indexing | O(n) queries |
| `ast.literal_eval` | Security vulnerability |

### 5.2 New Architecture: RocksDB Backend

```
┌─────────────────────────────────────────────────────────────────┐
│                      RaftNode                                    │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    State Machine                           │ │
│  │  (In-memory cache + RocksDB backend)                       │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    RaftLog (WAL)                           │ │
│  │  RocksDB Column Family: "raft_log"                         │ │
│  │  Key: index (uint64), Value: LogEntry (protobuf)          │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                    RaftMeta                                │ │
│  │  RocksDB Column Family: "raft_meta"                        │ │
│  │  Key: "current_term", Value: uint64                       │ │
│  │  Key: "voted_for", Value: node_id                         │ │
│  │  Key: "snapshot_index", Value: uint64                     │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      RocksDB                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  MemTable    │  │  WAL         │  │  SSTables    │          │
│  │  (In-Memory) │  │  (Sequential)│  │  (Compacted) │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│                                                                  │
│  Column Families:                                                │
│  - default:    Task data (key: task_id, value: Task protobuf)  │
│  - raft_log:   Raft log entries                                 │
│  - raft_meta:  Term, vote, snapshot metadata                    │
│  - indexes:    Secondary indexes (status, namespace, etc.)      │
└─────────────────────────────────────────────────────────────────┘
```

### 5.3 Implementation

```python
# storage_v2.py

import rocksdb
from typing import Optional, List, Iterator
from dataclasses import dataclass
import struct

class RaftStorageV2:
    """
    Production-grade Raft storage using RocksDB.
    
    Features:
    - O(1) log append
    - Atomic writes with WriteBatch
    - Automatic compaction
    - Snapshot support
    """
    
    def __init__(self, node_id: int, data_dir: str):
        self.node_id = node_id
        self.db_path = f"{data_dir}/node_{node_id}"
        
        # Column families for different data types
        self.column_families = {
            'default': rocksdb.ColumnFamilyOptions(),
            'raft_log': rocksdb.ColumnFamilyOptions(),
            'raft_meta': rocksdb.ColumnFamilyOptions(),
            'tasks': rocksdb.ColumnFamilyOptions(),
            'indexes': rocksdb.ColumnFamilyOptions(),
        }
        
        # Open database
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.create_missing_column_families = True
        
        self.db = rocksdb.DB(
            self.db_path,
            opts,
            column_families=self.column_families,
        )
        
        # Get column family handles
        self.cf_log = self.db.get_column_family(b'raft_log')
        self.cf_meta = self.db.get_column_family(b'raft_meta')
        self.cf_tasks = self.db.get_column_family(b'tasks')
        self.cf_indexes = self.db.get_column_family(b'indexes')
    
    # ========== Raft Meta ==========
    
    async def save_term(self, term: int) -> None:
        """Persist current term."""
        self.db.put(
            b'current_term',
            struct.pack('>Q', term),
            column_family=self.cf_meta,
        )
    
    async def load_term(self) -> int:
        """Load current term."""
        value = self.db.get(b'current_term', column_family=self.cf_meta)
        return struct.unpack('>Q', value)[0] if value else 0
    
    async def save_vote(self, voted_for: Optional[int]) -> None:
        """Persist vote."""
        value = struct.pack('>q', voted_for) if voted_for is not None else b''
        self.db.put(b'voted_for', value, column_family=self.cf_meta)
    
    async def load_vote(self) -> Optional[int]:
        """Load vote."""
        value = self.db.get(b'voted_for', column_family=self.cf_meta)
        if not value:
            return None
        return struct.unpack('>q', value)[0]
    
    # ========== Raft Log ==========
    
    async def append_log_entry(self, index: int, entry: LogEntry) -> None:
        """Append a log entry - O(1) operation."""
        key = struct.pack('>Q', index)
        value = entry.SerializeToString()
        self.db.put(key, value, column_family=self.cf_log)
    
    async def get_log_entry(self, index: int) -> Optional[LogEntry]:
        """Get a log entry by index."""
        key = struct.pack('>Q', index)
        value = self.db.get(key, column_family=self.cf_log)
        if not value:
            return None
        entry = LogEntry()
        entry.ParseFromString(value)
        return entry
    
    async def get_log_entries(
        self, 
        start: int, 
        end: int,
    ) -> List[LogEntry]:
        """Get a range of log entries."""
        entries = []
        start_key = struct.pack('>Q', start)
        end_key = struct.pack('>Q', end)
        
        it = self.db.iterkeys(column_family=self.cf_log)
        it.seek(start_key)
        
        for key, value in it:
            if key >= end_key:
                break
            entry = LogEntry()
            entry.ParseFromString(value)
            entries.append(entry)
        
        return entries
    
    async def truncate_log(self, from_index: int) -> None:
        """Truncate log from given index."""
        batch = rocksdb.WriteBatch()
        start_key = struct.pack('>Q', from_index)
        
        it = self.db.iterkeys(column_family=self.cf_log)
        it.seek(start_key)
        
        for key in it:
            batch.delete(key, column_family=self.cf_log)
        
        self.db.write(batch)
    
    async def get_last_log_index(self) -> int:
        """Get the last log index."""
        it = self.db.iterkeys(column_family=self.cf_log, reverse=True)
        try:
            it.seek_to_last()
            key = next(it)
            return struct.unpack('>Q', key)[0]
        except StopIteration:
            return 0
    
    # ========== Snapshot ==========
    
    async def save_snapshot(
        self,
        last_included_index: int,
        last_included_term: int,
        snapshot_data: bytes,
    ) -> None:
        """Save snapshot atomically."""
        batch = rocksdb.WriteBatch()
        
        # Save snapshot metadata
        batch.put(
            b'snapshot_index',
            struct.pack('>Q', last_included_index),
            column_family=self.cf_meta,
        )
        batch.put(
            b'snapshot_term',
            struct.pack('>Q', last_included_term),
            column_family=self.cf_meta,
        )
        
        # Save snapshot data
        batch.put(b'snapshot_data', snapshot_data, column_family=self.cf_meta)
        
        # Truncate log entries before snapshot
        start_key = struct.pack('>Q', 1)
        end_key = struct.pack('>Q', last_included_index + 1)
        
        it = self.db.iterkeys(column_family=self.cf_log)
        it.seek(start_key)
        
        for key in it:
            if key >= end_key:
                break
            batch.delete(key, column_family=self.cf_log)
        
        self.db.write(batch)
    
    async def load_snapshot(self) -> Optional[Tuple[int, int, bytes]]:
        """Load snapshot metadata and data."""
        index = self.db.get(b'snapshot_index', column_family=self.cf_meta)
        term = self.db.get(b'snapshot_term', column_family=self.cf_meta)
        data = self.db.get(b'snapshot_data', column_family=self.cf_meta)
        
        if not index or not term:
            return None
        
        return (
            struct.unpack('>Q', index)[0],
            struct.unpack('>Q', term)[0],
            data,
        )
    
    # ========== Task Storage (State Machine) ==========
    
    async def save_task(self, task: Task) -> None:
        """Save a task to the state machine."""
        # Primary key: task_id
        key = task.id.SerializeToString()
        value = task.SerializeToString()
        self.db.put(key, value, column_family=self.cf_tasks)
        
        # Update indexes
        await self._update_task_indexes(task)
    
    async def get_task(self, task_id: TaskID) -> Optional[Task]:
        """Get a task by ID."""
        key = task_id.SerializeToString()
        value = self.db.get(key, column_family=self.cf_tasks)
        if not value:
            return None
        task = Task()
        task.ParseFromString(value)
        return task
    
    async def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
        namespace: Optional[str] = None,
        limit: int = 100,
        offset: bytes = b'',
    ) -> List[Task]:
        """List tasks with filtering."""
        # Use appropriate index
        if status is not None:
            prefix = f"status:{status}:".encode()
        elif namespace is not None:
            prefix = f"ns:{namespace}:".encode()
        else:
            prefix = b''
        
        tasks = []
        it = self.db.iteritems(column_family=self.cf_indexes)
        
        if offset:
            it.seek(offset)
        else:
            it.seek(prefix)
        
        for key, value in it:
            if not key.startswith(prefix):
                break
            task = Task()
            task.ParseFromString(value)
            tasks.append(task)
            if len(tasks) >= limit:
                break
        
        return tasks
    
    async def _update_task_indexes(self, task: Task) -> None:
        """Update secondary indexes for a task."""
        batch = rocksdb.WriteBatch()
        task_key = task.id.SerializeToString()
        task_value = task.SerializeToString()
        
        # Status index
        batch.put(
            f"status:{task.status}:{task_key}".encode(),
            task_value,
            column_family=self.cf_indexes,
        )
        
        # Namespace index
        batch.put(
            f"ns:{task.namespace}:{task_key}".encode(),
            task_value,
            column_family=self.cf_indexes,
        )
        
        # Node index (if assigned)
        if task.assigned_node:
            batch.put(
                f"node:{task.assigned_node}:{task_key}".encode(),
                task_value,
                column_family=self.cf_indexes,
            )
        
        self.db.write(batch)
```

---

## 6. Network Layer

### 6.1 gRPC Service Definition

```protobuf
// aethergrid_service.proto

syntax = "proto3";

package aethergrid;

import "aethergrid.proto";

// ============== Client API Service ==============

service TaskService {
    // Task management
    rpc SubmitTask(SubmitTaskRequest) returns (SubmitTaskResponse);
    rpc GetTask(GetTaskRequest) returns (GetTaskResponse);
    rpc ListTasks(ListTasksRequest) returns (ListTasksResponse);
    rpc CancelTask(CancelTaskRequest) returns (CancelTaskResponse);
    
    // Task logs
    rpc GetTaskLogs(GetTaskLogsRequest) returns (stream LogEntry);
    
    // Cluster info
    rpc GetClusterInfo(GetClusterInfoRequest) returns (GetClusterInfoResponse);
    rpc ListNodes(ListNodesRequest) returns (ListNodesResponse);
}

// ============== Internal Raft Service ==============

service RaftService {
    rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
    rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
    rpc InstallSnapshot(stream InstallSnapshotRequest) 
        returns (InstallSnapshotResponse);
}

// ============== Worker Agent Service ==============

service WorkerService {
    // Called by leader to assign tasks
    rpc AssignTask(AssignTaskRequest) returns (AssignTaskResponse);
    rpc CancelTask(CancelTaskRequest) returns (CancelTaskResponse);
    
    // Called by worker to report status
    rpc ReportTaskStatus(ReportTaskStatusRequest) 
        returns (ReportTaskStatusResponse);
    rpc StreamHeartbeats(stream HeartbeatRequest) 
        returns (stream HeartbeatResponse);
}
```

### 6.2 Python gRPC Server

```python
# grpc_server.py

import grpc
from concurrent import futures
import asyncio
from typing import AsyncIterator

import aethergrid_pb2
import aethergrid_pb2_grpc

class TaskServicer(aethergrid_pb2_grpc.TaskServiceServicer):
    """gRPC service for client-facing Task API."""
    
    def __init__(self, cluster: 'Cluster'):
        self.cluster = cluster
    
    async def SubmitTask(
        self,
        request: aethergrid_pb2.SubmitTaskRequest,
        context: grpc.ServicerContext,
    ) -> aethergrid_pb2.SubmitTaskResponse:
        """Submit a new task."""
        # Extract auth info
        user = self._extract_user(context)
        
        # Route to appropriate shard
        shard_id = self._route_to_shard(request.spec.name)
        
        # Submit to shard leader
        task = await self.cluster.submit_task(
            shard_id=shard_id,
            spec=request.spec,
            submitted_by=user,
        )
        
        return aethergrid_pb2.SubmitTaskResponse(task=task)
    
    async def GetTask(
        self,
        request: aethergrid_pb2.GetTaskRequest,
        context: grpc.ServicerContext,
    ) -> aethergrid_pb2.GetTaskResponse:
        """Get task by ID."""
        # Can read from any replica (eventually consistent)
        task = await self.cluster.get_task(request.task_id)
        
        if task is None:
            await context.abort(
                grpc.StatusCode.NOT_FOUND,
                f"Task {request.task_id} not found",
            )
        
        return aethergrid_pb2.GetTaskResponse(task=task)
    
    async def ListTasks(
        self,
        request: aethergrid_pb2.ListTasksRequest,
        context: grpc.ServicerContext,
    ) -> aethergrid_pb2.ListTasksResponse:
        """List tasks with filtering."""
        # Scatter-gather across shards
        tasks = await self.cluster.list_tasks(
            namespace=request.namespace,
            status_filter=request.status_filter,
            label_selector=request.label_selector,
            page_size=request.page_size,
            page_token=request.page_token,
        )
        
        return aethergrid_pb2.ListTasksResponse(
            tasks=tasks.tasks,
            next_page_token=tasks.next_page_token,
        )
    
    async def GetTaskLogs(
        self,
        request: aethergrid_pb2.GetTaskLogsRequest,
        context: grpc.ServicerContext,
    ) -> AsyncIterator[aethergrid_pb2.LogEntry]:
        """Stream task logs."""
        async for entry in self.cluster.stream_task_logs(
            task_id=request.task_id,
            follow=request.follow,
            tail_lines=request.tail_lines,
        ):
            yield entry


class RaftServicer(aethergrid_pb2_grpc.RaftServiceServicer):
    """gRPC service for internal Raft communication."""
    
    def __init__(self, node: 'RaftNodeV2'):
        self.node = node
    
    async def AppendEntries(
        self,
        request: aethergrid_pb2.AppendEntriesRequest,
        context: grpc.ServicerContext,
    ) -> aethergrid_pb2.AppendEntriesResponse:
        """Handle AppendEntries RPC."""
        response = await self.node.handle_append_entries(request)
        return response
    
    async def RequestVote(
        self,
        request: aethergrid_pb2.RequestVoteRequest,
        context: grpc.ServicerContext,
    ) -> aethergrid_pb2.RequestVoteResponse:
        """Handle RequestVote RPC."""
        response = await self.node.handle_request_vote(request)
        return response
    
    async def InstallSnapshot(
        self,
        request_iterator: AsyncIterator[aethergrid_pb2.InstallSnapshotRequest],
        context: grpc.ServicerContext,
    ) -> aethergrid_pb2.InstallSnapshotResponse:
        """Handle streaming InstallSnapshot RPC."""
        chunks = []
        async for request in request_iterator:
            chunks.append(request.snapshot_data)
            
            if request.done:
                break
        
        snapshot_data = b''.join(chunks)
        response = await self.node.handle_install_snapshot(
            term=request.term,
            leader_id=request.leader_id,
            last_included_index=request.last_included_index,
            last_included_term=request.last_included_term,
            snapshot_data=snapshot_data,
        )
        return response


async def serve(cluster: 'Cluster', port: int = 50051) -> None:
    """Start the gRPC server."""
    server = grpc.aio.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_receive_message_length', 64 * 1024 * 1024),  # 64MB
            ('grpc.max_send_message_length', 64 * 1024 * 1024),
        ],
    )
    
    # Register services
    aethergrid_pb2_grpc.add_TaskServiceServicer_to_server(
        TaskServicer(cluster), server
    )
    aethergrid_pb2_grpc.add_RaftServiceServicer_to_server(
        RaftServicer(cluster.local_node), server
    )
    
    # Enable TLS
    if cluster.config.tls_enabled:
        with open(cluster.config.cert_file, 'rb') as f:
            cert = f.read()
        with open(cluster.config.key_file, 'rb') as f:
            key = f.read()
        with open(cluster.config.ca_file, 'rb') as f:
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
    await server.wait_for_termination()
```

---

## 7. Security Model

### 7.1 Authentication & Authorization

```
┌─────────────────────────────────────────────────────────────────┐
│                     Security Layers                              │
│                                                                  │
│  Layer 1: mTLS (Node-to-Node)                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  - All Raft traffic encrypted with mutual TLS              │ │
│  │  - Each node has unique certificate with node_id           │ │
│  │  - Certificate rotation via Raft consensus                 │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Layer 2: JWT (Client-to-Gateway)                               │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  - Clients authenticate with JWT tokens                    │ │
│  │  - Tokens contain: user_id, roles, namespaces              │ │
│  │  - Short-lived tokens (1 hour) with refresh tokens         │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  Layer 3: RBAC (Authorization)                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │  - Roles: admin, developer, viewer                         │ │
│  │  - Namespace isolation                                      │ │
│  │  - Resource quotas per user/namespace                       │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 7.2 RBAC Implementation

```python
# security.py

from dataclasses import dataclass
from typing import Set, Optional
from enum import Enum

class Role(Enum):
    ADMIN = "admin"
    DEVELOPER = "developer"
    VIEWER = "viewer"

@dataclass
class User:
    id: str
    roles: Set[Role]
    namespaces: Set[str]  # Empty = all namespaces
    quotas: 'ResourceQuota'

@dataclass
class ResourceQuota:
    max_tasks: int
    max_cpu_millis: int
    max_memory_bytes: int
    max_gpu_count: int

class Authorizer:
    """RBAC authorization for task operations."""
    
    def __init__(self):
        self.users: Dict[str, User] = {}
    
    def can_submit_task(self, user: User, namespace: str) -> bool:
        """Check if user can submit tasks to namespace."""
        # Check namespace access
        if user.namespaces and namespace not in user.namespaces:
            return False
        
        # Check role
        if Role.DEVELOPER not in user.roles and Role.ADMIN not in user.roles:
            return False
        
        return True
    
    def can_get_task(self, user: User, task: Task) -> bool:
        """Check if user can view task."""
        # Check namespace access
        if user.namespaces and task.namespace not in user.namespaces:
            return False
        
        # All roles can view
        return True
    
    def can_cancel_task(self, user: User, task: Task) -> bool:
        """Check if user can cancel task."""
        # Check namespace access
        if user.namespaces and task.namespace not in user.namespaces:
            return False
        
        # Admin can cancel any task
        if Role.ADMIN in user.roles:
            return True
        
        # Developer can cancel their own tasks
        if Role.DEVELOPER in user.roles:
            return task.submitted_by == user.id
        
        return False
    
    def check_quota(
        self, 
        user: User, 
        resources: ResourceSpec,
    ) -> bool:
        """Check if user has quota for resources."""
        quota = user.quotas
        return (
            resources.cpu_millis <= quota.max_cpu_millis and
            resources.memory_bytes <= quota.max_memory_bytes and
            resources.gpu_count <= quota.max_gpu_count
        )
```

### 7.3 Secure Snapshot Handling

```python
# Replace ast.literal_eval with protobuf deserialization

class SecureSnapshot:
    """Secure snapshot serialization/deserialization."""
    
    @staticmethod
    def serialize(state: ProcessTableState) -> bytes:
        """Serialize state to protobuf."""
        snapshot = Snapshot()
        
        # Add tasks
        for task in state.tasks.values():
            snapshot.process_table.tasks.append(task)
        
        # Add nodes
        for node in state.nodes.values():
            snapshot.cluster_state.nodes.append(node)
        
        snapshot.process_table.next_task_id = state.next_task_sequence
        
        return snapshot.SerializeToString()
    
    @staticmethod
    def deserialize(data: bytes) -> ProcessTableState:
        """Deserialize state from protobuf."""
        # Validate size limit
        if len(data) > MAX_SNAPSHOT_SIZE:
            raise ValueError(f"Snapshot too large: {len(data)} > {MAX_SNAPSHOT_SIZE}")
        
        snapshot = Snapshot()
        
        try:
            # Protobuf parsing is safe - no code execution
            snapshot.ParseFromString(data)
        except Exception as e:
            raise ValueError(f"Invalid snapshot data: {e}")
        
        return ProcessTableState.from_snapshot(snapshot)
```

---

## 8. Scalability Strategy

### 8.1 Multi-Raft Sharding

```
┌─────────────────────────────────────────────────────────────────┐
│                    Multi-Raft Architecture                       │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Meta Raft Group                           ││
│  │  - Cluster configuration                                     ││
│  │  - Shard assignments                                         ││
│  │  - Node membership                                           ││
│  │  - Global policies                                           ││
│  └─────────────────────────────────────────────────────────────┘│
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    Shard Router                              ││
│  │  shard_id = hash(task_name) % num_shards                    ││
│  │  or                                                         ││
│  │  shard_id = hash(namespace) % num_shards                    ││
│  └─────────────────────────────────────────────────────────────┘│
│                              │                                   │
│       ┌──────────────────────┼──────────────────────┐           │
│       ▼                      ▼                      ▼           │
│  ┌─────────────┐       ┌─────────────┐       ┌─────────────┐   │
│  │  Shard 0    │       │  Shard 1    │       │  Shard N    │   │
│  │             │       │             │       │             │   │
│  │  Tasks:     │       │  Tasks:     │       │  Tasks:     │   │
│  │  0 - 1M     │       │  1M - 2M    │       │  NM - (N+1)M│   │
│  │             │       │             │       │             │   │
│  │  Leader: N1│       │  Leader: N3 │       │  Leader: N2 │   │
│  │  F1: N2    │       │  F1: N1     │       │  F1: N1     │   │
│  │  F2: N3    │       │  F2: N2     │       │  F2: N3     │   │
│  └─────────────┘       └─────────────┘       └─────────────┘   │
│                                                                  │
│  Each shard is an independent Raft group with its own leader.   │
│  Total write throughput = N shards × single-shard throughput    │
└─────────────────────────────────────────────────────────────────┘
```

### 8.2 Shard Router Implementation

```python
# sharding.py

from typing import Dict, List, Optional
import hashlib
from dataclasses import dataclass

@dataclass
class ShardConfig:
    shard_id: int
    leader_node: NodeID
    replicas: List[NodeID]
    key_range: Tuple[int, int]

class ShardRouter:
    """Routes requests to appropriate shard."""
    
    def __init__(self, meta_client: 'MetaRaftClient'):
        self.meta_client = meta_client
        self.shard_cache: Dict[int, ShardConfig] = {}
        self.num_shards = 16  # Configurable
    
    def route_task(self, task_name: str, namespace: str) -> int:
        """Determine which shard owns a task."""
        # Hash-based routing
        key = f"{namespace}/{task_name}"
        hash_val = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        return hash_val % self.num_shards
    
    async def get_shard_leader(self, shard_id: int) -> NodeID:
        """Get the current leader for a shard."""
        # Check cache
        if shard_id in self.shard_cache:
            return self.shard_cache[shard_id].leader_node
        
        # Fetch from meta raft
        config = await self.meta_client.get_shard_config(shard_id)
        self.shard_cache[shard_id] = config
        
        return config.leader_node
    
    async def submit_task(self, spec: SubmitTaskCommand) -> Task:
        """Submit task to appropriate shard."""
        # Route to shard
        shard_id = self.route_task(spec.name, spec.namespace)
        
        # Get shard leader
        leader = await self.get_shard_leader(shard_id)
        
        # Submit to leader
        client = await self.get_client(leader)
        task = await client.submit_task(spec)
        
        return task
    
    async def list_tasks(
        self,
        namespace: Optional[str] = None,
        status: Optional[TaskStatus] = None,
    ) -> List[Task]:
        """Scatter-gather task listing across shards."""
        # Query all shards in parallel
        tasks = []
        
        async def query_shard(shard_id: int) -> List[Task]:
            leader = await self.get_shard_leader(shard_id)
            client = await self.get_client(leader)
            return await client.list_tasks(namespace, status)
        
        results = await asyncio.gather(*[
            query_shard(i) for i in range(self.num_shards)
        ])
        
        for shard_tasks in results:
            tasks.extend(shard_tasks)
        
        return tasks
```

### 8.3 Read Replicas

```python
# read_replica.py

class ReadReplica:
    """
    Allows followers to serve read requests.
    
    Trade-off: May return slightly stale data.
    """
    
    def __init__(self, node: 'RaftNodeV2'):
        self.node = node
    
    async def get_task(self, task_id: TaskID) -> Optional[Task]:
        """
        Get task from local state.
        
        May return stale data if not leader.
        """
        # Check if we're leader
        if self.node.is_leader():
            # Linearizable read via ReadIndex
            return await self.node.linearizable_read(task_id)
        else:
            # Eventually consistent read from local state
            return self.node.state_machine.get_task(task_id)
    
    async def list_tasks(
        self,
        namespace: str,
        status: Optional[TaskStatus] = None,
    ) -> List[Task]:
        """
        List tasks from local state.
        
        Followers can serve this without contacting leader.
        """
        return self.node.state_machine.list_tasks(
            namespace=namespace,
            status=status,
        )
```

### 8.4 Scale Projections

| Configuration | Shards | Nodes | Throughput (writes/sec) | Storage Capacity |
|---------------|--------|-------|------------------------|------------------|
| Small | 4 | 12 | 400-1,200 | 100K tasks |
| Medium | 16 | 48 | 1,600-4,800 | 1M tasks |
| Large | 64 | 192 | 6,400-19,200 | 10M tasks |
| X-Large | 256 | 768 | 25,600-76,800 | 100M tasks |

*Assumes 3 replicas per shard, 100-300 writes/sec per shard*

---

## 9. Observability & Operations

### 9.1 Metrics

```python
# metrics.py

from prometheus_client import Counter, Histogram, Gauge, Info

# Raft Metrics
RAFT_TERM = Gauge('aethergrid_raft_term', 'Current Raft term')
RAFT_STATE = Gauge('aethergrid_raft_state', 'Current Raft state (0=follower, 1=candidate, 2=leader)')
RAFT_COMMIT_INDEX = Gauge('aethergrid_raft_commit_index', 'Current commit index')
RAFT_LAST_APPLIED = Gauge('aethergrid_raft_last_applied', 'Last applied log index')

# Log Metrics
RAFT_LOG_LENGTH = Gauge('aethergrid_raft_log_length', 'Number of entries in log')
RAFT_LOG_APPENDS = Counter('aethergrid_raft_log_appends_total', 'Total log appends')
RAFT_LOG_APPEND_LATENCY = Histogram(
    'aethergrid_raft_log_append_latency_seconds',
    'Log append latency',
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# Election Metrics
RAFT_ELECTIONS = Counter('aethergrid_raft_elections_total', 'Total elections')
RAFT_ELECTION_TIMEOUTS = Counter('aethergrid_raft_election_timeouts_total', 'Election timeouts')

# Task Metrics
TASKS_TOTAL = Gauge('aethergrid_tasks_total', 'Total tasks', ['status', 'namespace'])
TASKS_SUBMITTED = Counter('aethergrid_tasks_submitted_total', 'Tasks submitted')
TASKS_COMPLETED = Counter('aethergrid_tasks_completed_total', 'Tasks completed', ['status'])
TASK_LATENCY = Histogram(
    'aethergrid_task_duration_seconds',
    'Task execution duration',
    buckets=[1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600],
)

# Node Metrics
NODES_TOTAL = Gauge('aethergrid_nodes_total', 'Total nodes', ['status'])
NODE_RESOURCES = Gauge(
    'aethergrid_node_resources',
    'Node resource usage',
    ['node_id', 'resource_type'],
)

# gRPC Metrics
GRPC_REQUESTS = Counter(
    'aethergrid_grpc_requests_total',
    'gRPC requests',
    ['method', 'code'],
)
GRPC_LATENCY = Histogram(
    'aethergrid_grpc_latency_seconds',
    'gRPC request latency',
    ['method'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)
```

### 9.2 Health Endpoints

```python
# health.py

from fastapi import FastAPI, Response
import asyncio

app = FastAPI()

class HealthChecker:
    def __init__(self, node: 'RaftNodeV2'):
        self.node = node
    
    async def is_healthy(self) -> bool:
        """Check if node is healthy."""
        # Check if we've received recent heartbeat (if follower)
        if not self.node.is_leader():
            time_since_heartbeat = asyncio.get_event_loop().time() - self.node.last_heartbeat
            if time_since_heartbeat > self.node.election_timeout_max * 3:
                return False
        
        # Check storage
        if not await self.node.storage.is_healthy():
            return False
        
        return True
    
    async def is_ready(self) -> bool:
        """Check if node is ready to serve requests."""
        # Must be part of a cluster with leader
        if self.node.is_leader():
            return True
        
        # Follower must know the leader
        return self.node.known_leader is not None

@app.get("/health")
async def health():
    """Liveness probe."""
    if await health_checker.is_healthy():
        return Response(status_code=200, content="OK")
    return Response(status_code=503, content="Unhealthy")

@app.get("/ready")
async def ready():
    """Readiness probe."""
    if await health_checker.is_ready():
        return Response(status_code=200, content="Ready")
    return Response(status_code=503, content="Not Ready")

@app.get("/metrics")
async def metrics():
    """Prometheus metrics."""
    from prometheus_client import generate_latest
    return Response(
        content=generate_latest(),
        media_type="text/plain",
    )
```

---

## 10. Implementation Roadmap

### Phase 1: Foundation (Weeks 1-4)

**Goal**: Fix critical issues and establish production-ready base.

| Week | Tasks | Deliverables |
|------|-------|--------------|
| 1 | Storage Layer Rewrite | RocksDB integration, WAL implementation |
| 2 | Protobuf Schema | Define all message types, generate Python/Go code |
| 3 | gRPC Network Layer | Replace asyncio queues with gRPC services |
| 4 | Security Hardening | Remove `ast.literal_eval`, add mTLS for node-to-node |

**Milestone**: Single-node cluster passes all existing tests with new storage/network.

### Phase 2: Core Features (Weeks 5-8)

**Goal**: Implement Decentralized OS primitives.

| Week | Tasks | Deliverables |
|------|-------|--------------|
| 5 | Task State Machine | ProcessTableState with task lifecycle |
| 6 | Scheduler | Basic round-robin scheduler with resource tracking |
| 7 | Worker Agent | Docker runtime integration, task execution |
| 8 | Client SDK | Python SDK with submit/get/list/cancel operations |

**Milestone**: Can submit, schedule, and execute tasks on a single-shard cluster.

### Phase 3: Scalability (Weeks 9-12)

**Goal**: Multi-Raft sharding for horizontal scaling.

| Week | Tasks | Deliverables |
|------|-------|--------------|
| 9 | Meta Raft | Cluster config, shard assignments |
| 10 | Shard Router | Hash-based routing, scatter-gather queries |
| 11 | Read Replicas | Follower reads, consistency levels |
| 12 | Cluster Rebalancing | Add/remove shards, data migration |

**Milestone**: 16-shard cluster with 10K+ writes/sec throughput.

### Phase 4: Production Hardening (Weeks 13-16)

**Goal**: Production-ready operations.

| Week | Tasks | Deliverables |
|------|-------|--------------|
| 13 | Observability | Prometheus metrics, structured logging |
| 14 | Backup/Restore | Snapshot backup, disaster recovery |
| 15 | Chaos Testing | Network partition tests, failure injection |
| 16 | Documentation | API docs, operations runbook |

**Milestone**: Passes chaos testing with automatic failover and recovery.

### Phase 5: Advanced Features (Weeks 17-20)

**Goal**: Enterprise features.

| Week | Tasks | Deliverables |
|------|-------|--------------|
| 17 | Authentication | JWT auth, RBAC |
| 18 | Resource Quotas | Per-namespace/user quotas |
| 19 | Task Dependencies | DAG workflows, dependencies |
| 20 | Web UI | Dashboard for task management |

**Milestone**: Production-ready Decentralized OS platform.

---

## Appendix A: File Structure

```
aethergrid/
├── proto/
│   ├── aethergrid.proto          # Core message types
│   └── aethergrid_service.proto  # gRPC service definitions
│
├── aethergrid/
│   ├── __init__.py
│   ├── node/
│   │   ├── __init__.py
│   │   ├── raft_node.py          # Core Raft implementation
│   │   ├── state_machine.py      # Process table state machine
│   │   └── snapshot.py           # Snapshot management
│   │
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── rocksdb_store.py      # RocksDB backend
│   │   └── wal.py                # Write-ahead log
│   │
│   ├── network/
│   │   ├── __init__.py
│   │   ├── grpc_server.py        # gRPC service implementations
│   │   ├── grpc_client.py        # gRPC client
│   │   └── tls.py                # TLS certificate management
│   │
│   ├── scheduler/
│   │   ├── __init__.py
│   │   ├── scheduler.py          # Task scheduler
│   │   ├── strategies.py         # Scheduling strategies
│   │   └── resource_tracker.py   # Resource tracking
│   │
│   ├── worker/
│   │   ├── __init__.py
│   │   ├── agent.py              # Worker agent
│   │   ├── runtime.py            # Runtime abstraction
│   │   ├── docker_runtime.py     # Docker runtime
│   │   └── wasm_runtime.py       # WASM runtime
│   │
│   ├── sharding/
│   │   ├── __init__.py
│   │   ├── shard_router.py       # Request routing
│   │   └── meta_raft.py          # Meta Raft group
│   │
│   ├── security/
│   │   ├── __init__.py
│   │   ├── auth.py               # Authentication
│   │   ├── rbac.py               # Authorization
│   │   └── tls.py                # TLS management
│   │
│   ├── observability/
│   │   ├── __init__.py
│   │   ├── metrics.py            # Prometheus metrics
│   │   ├── health.py             # Health endpoints
│   │   └── logging.py            # Structured logging
│   │
│   └── cli/
│       ├── __init__.py
│       └── main.py               # CLI tool
│
├── sdk/
│   ├── python/
│   │   ├── aethergrid_sdk/
│   │   │   ├── __init__.py
│   │   │   ├── client.py
│   │   │   └── models.py
│   │   └── setup.py
│   │
│   └── go/
│       ├── aethergrid/
│       │   ├── client.go
│       │   └── models.go
│       └── go.mod
│
├── tests/
│   ├── unit/
│   ├── integration/
│   └── chaos/
│
├── deploy/
│   ├── docker/
│   ├── kubernetes/
│   └── terraform/
│
├── docs/
│   ├── api.md
│   ├── architecture.md
│   └── operations.md
│
├── Makefile
├── pyproject.toml
└── README.md
```

---

## Appendix B: Dependencies

```toml
# pyproject.toml

[project]
name = "aethergrid"
version = "2.0.0"
requires-python = ">=3.10"

dependencies = [
    "grpcio>=1.60.0",
    "grpcio-tools>=1.60.0",
    "protobuf>=4.25.0",
    "python-rocksdb>=0.7.0",
    "prometheus-client>=0.19.0",
    "fastapi>=0.109.0",
    "uvicorn>=0.27.0",
    "pydantic>=2.5.0",
    "docker>=7.0.0",
    "cryptography>=42.0.0",
    "jwt>=2.8.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.4.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=4.1.0",
    "mypy>=1.8.0",
    "black>=24.1.0",
    "ruff>=0.2.0",
    "hypothesis>=6.98.0",
]
```

---

*End of Architecture Document*
