"""AetherGrid v2.0 - Decentralized OS & Task Orchestration Platform.

AetherGrid transforms a Raft-based replicated state machine into a
Decentralized OS Kernel that manages:

- Global Process Table: Distributed, strongly-consistent registry of all tasks
- Resource Allocation: CPU, memory, GPU scheduling across cluster nodes
- Task Lifecycle Management: Submit → Schedule → Execute → Monitor → Complete
- Fault Tolerance: Automatic task rescheduling on node failures

Key Features:
- Fencing Tokens: Prevent zombie workers from corrupting state
- Log-Based Leases: Deterministic time using log index (no wall clock)
- SQLite/RocksDB Storage: Production-grade persistence with WAL
- Multi-Raft Ready: Architecture supports horizontal scaling

Example Usage:
    from aethergrid import RaftNode, ProcessTableState, TaskID
    
    # Create state machine
    state = ProcessTableState(shard_id=0)
    
    # Submit a task
    result = state.apply_command("submit_task", {
        "name": "my-task",
        "namespace": "default",
        "image": "alpine",
        "args": ["echo", "hello"],
    })
    
    # Task has fencing token for zombie worker protection
    task = result.task
    print(f"Task {task.id} has fencing token {task.fencing_token}")
"""

__version__ = "2.0.0"

from .core import (
    # State Machine
    ProcessTableState,
    Task,
    TaskID,
    NodeID,
    TaskStatus,
    NodeHealth,
    ResourceSpec,
    CommandResult,
    
    # Raft
    RaftNode,
    RaftState,
    RaftConfig,
)

from .storage import (
    StorageBackend,
    SQLiteWALStorage,
    MemoryStorage,
    LogEntry,
    Snapshot,
)

from .worker import (
    WorkerAgent,
    TaskHandle,
    FencingTokenValidator,
)

from .workflow import (
    WorkflowEngine,
    Workflow,
    WorkflowTask,
    define_workflow,
    run_workflow,
)

from .config import (
    ConfigLoader,
    load_config,
    get_secret,
)

from .testing import (
    benchmark_task_submission,
    ChaosMonkey,
)

__all__ = [
    # Version
    "__version__",
    
    # State Machine
    "ProcessTableState",
    "Task",
    "TaskID",
    "NodeID",
    "TaskStatus",
    "NodeHealth",
    "ResourceSpec",
    "CommandResult",
    
    # Raft
    "RaftNode",
    "RaftState",
    "RaftConfig",
    
    # Storage
    "StorageBackend",
    "SQLiteWALStorage",
    "MemoryStorage",
    "LogEntry",
    "Snapshot",
    
    # Worker
    "WorkerAgent",
    "TaskHandle",
    "FencingTokenValidator",
    
    # Workflow
    "WorkflowEngine",
    "Workflow",
    "WorkflowTask",
    "define_workflow",
    "run_workflow",
    
    # Config
    "ConfigLoader",
    "load_config",
    "get_secret",
    
    # Testing
    "benchmark_task_submission",
    "ChaosMonkey",
]
