# AetherGrid: Roadmap to Distributed AI/ML Compute System

## Reality Check

### Can This Codebase Support AI/ML Compute?

**Short answer: Yes, but with targeted work.**

The foundation is solid. The Raft consensus, fencing tokens, and state machine design are production-quality concepts. However, you'll hit real limitations if you try to run actual training jobs today.

### Fundamental Limitations You'll Hit

| Limitation | Impact on AI/ML | Severity |
|------------|-----------------|----------|
| No real networking | Can't run multi-node cluster | **BLOCKER** |
| No GPU tracking | Can't schedule GPU jobs | **BLOCKER** |
| No checkpointing | Long jobs lose progress on failure | **HIGH** |
| No data management | No way to mount datasets | **HIGH** |
| No container isolation | Jobs run as native processes | **MEDIUM** |

**Note:** As of Step 0.1, local execution works. The worker agent can run training-like jobs as native processes.

### What's Solid (Keep As-Is)

- **Raft consensus core** (`raft_node.py`) - Well-designed, proper commit semantics
- **Fencing tokens** (`state_machine.py`) - Zombie worker protection is critical for AI jobs
- **State machine** (`state_machine.py`) - Task lifecycle model works for jobs
- **Storage abstraction** (`backend.py`) - SQLite WAL is fine for metadata
- **Log-based leases** - Deterministic time is the right approach

### What's Broken (Fix First)

- **Transport layer** - Uses in-memory queues, not real gRPC
- **Worker execution** - Works for local processes, needs Docker/containerd integration for isolation
- **Resource tracking** - GPU count field exists but isn't wired to anything

---

## Incremental Plan

The plan is structured in phases. Each phase produces something testable.

---

## Phase 0: Validate Core Execution (FIRST) ✅ COMPLETE

Before any infrastructure work, validate that the current system can execute a training-like job locally.

### Step 0.1: Run Local Execution Test ✅ Done

- [x] Create mock training script (`examples/train_mock.py`)
- [x] Create local execution test (`verify_local_execution.py`)
- [x] Run the test and verify SUCCESS

**What this validates:**
- State machine task lifecycle works
- Worker agent can execute processes
- Fencing tokens prevent zombie workers
- Checkpoints are created

**Run:**
```bash
# Start cluster
aether cluster start

# Submit task
aether submit --name mock-training --image python3 --args "/path/to/train_mock.py,--epochs,3"

# Check status
aether status 0:0

# View logs
aether logs 0:0
```

**Expected output:**
```
Submitted task 0:0
...
status: "succeeded"
```

**If this fails:** Do not proceed to networking. Fix the core execution model first.

**Files:**
- `examples/train_mock.py` - Mock AI training script
- `aethergrid/cli.py` - CLI commands (submit, status, logs)
- `scripts/dev_cluster.py` - Development cluster manager

---

## Phase 1: Real Networking (CRITICAL) ✅ IN PROGRESS

Before any distributed work, you need real network communication between nodes.

### Step 1.1: Wire Up gRPC Server ✅ Done

- [x] Generate Python code from `proto/aethergrid.proto`
- [x] Create `aethergrid/grpc_services.py` with actual gRPC service implementation
- [x] Implement `TaskService` with SubmitTask, GetTask, GetLogs, ListTasks
- [x] Add node startup that binds to a port
- [x] CLI uses gRPC client for submit/status/logs commands

**Test:** Submit task via CLI, verify it runs and status/logs work.

**Validation Commands:**
```bash
# Start the cluster
aether cluster start

# Submit a task via gRPC
aether submit --name mock-training --image python3 --args "/path/to/train_mock.py,--epochs,3"

# Check status
aether status 0:0

# View logs
aether logs 0:0

# Stop cluster
aether cluster stop
```

**Files modified:**
- `proto/aethergrid.proto` - Added TaskService, WorkerService definitions
- `aethergrid/grpc_services.py` - gRPC service implementation
- `aethergrid/generated/aethergrid_pb2.py` - Generated protobuf code
- `aethergrid/generated/aethergrid_pb2_grpc.py` - Generated gRPC stubs
- `scripts/dev_cluster.py` - gRPC-based cluster manager
- `aethergrid/cli.py` - CLI using gRPC client

### Step 1.2: Replace In-Memory Queues with gRPC Calls

- [ ] Modify `RaftNode` to use gRPC client instead of `send_message` callback
- [ ] Add peer address configuration (node_id -> host:port mapping)
- [ ] Handle connection failures with retries

**Test:** Run 3-node cluster, kill leader, verify new leader elected.

**Files to modify:**
- `aethergrid/core/raft_node.py`

### Step 1.3: Add Basic Health Endpoints

- [ ] Add `/health` endpoint (is node alive?)
- [ ] Add `/ready` endpoint (is node ready to serve?)
- [ ] Add `/metrics` endpoint (Prometheus format)

**Test:** Curl endpoints, verify responses.

**Files to create:**
- `aethergrid/api/health.py`

---

## Phase 2: Actually Run Jobs

Now that nodes can communicate, make them do real work.

### Step 2.1: Execute Docker Containers

- [ ] Implement `_run_docker()` in `WorkerAgent` to actually run containers
- [ ] Add container logging (capture stdout/stderr)
- [ ] Add container cleanup on task completion

**Test:** Submit a task that runs `alpine:latest echo hello`, verify output.

**Files to modify:**
- `aethergrid/worker/agent.py`

### Step 2.2: Add Task Submission API

- [ ] Create REST API endpoint `POST /tasks` to submit jobs
- [ ] Create `GET /tasks/{id}` to check status
- [ ] Create `GET /tasks/{id}/logs` to stream logs

**Test:** Submit task via curl, poll status, retrieve logs.

**Files to create:**
- `aethergrid/api/tasks.py`

### Step 2.3: Wire Leader to Workers

- [ ] Leader tracks registered workers
- [ ] Leader assigns tasks to workers (simple round-robin)
- [ ] Workers report status back to leader via gRPC

**Test:** Submit task, verify it runs on a worker node.

**Files to modify:**
- `aethergrid/core/raft_node.py` (scheduling logic)
- `aethergrid/worker/agent.py` (status reporting)

---

## Phase 3: GPU Support

Now add GPU scheduling capability.

### Step 3.1: Track GPU Resources

- [ ] Worker reports available GPUs in heartbeat
- [ ] State machine tracks GPU allocation per node
- [ ] Task spec includes GPU requirement

**Test:** Register worker with 2 GPUs, verify state machine tracks them.

**Files to modify:**
- `aethergrid/core/state_machine.py`
- `aethergrid/worker/agent.py`

### Step 3.2: GPU-Aware Scheduling

- [ ] Scheduler only assigns GPU tasks to GPU nodes
- [ ] Track GPU memory if possible (nvidia-smi integration)
- [ ] Prevent over-allocation

**Test:** Submit GPU task, verify it only schedules on GPU worker.

**Files to modify:**
- `aethergrid/core/state_machine.py`

### Step 3.3: GPU Passthrough to Container

- [ ] Add `--gpus` flag to Docker run command
- [ ] Verify GPU is visible inside container

**Test:** Run `nvidia-smi` inside container, verify output.

**Files to modify:**
- `aethergrid/worker/agent.py`

---

## Phase 4: AI/ML Job Features

Now add features specific to AI workloads.

### Step 4.1: Job Checkpointing (Basic)

- [ ] Define checkpoint directory convention (e.g., `/workspace/checkpoints/`)
- [ ] Worker syncs checkpoints to shared storage on completion/failure
- [ ] Task can resume from latest checkpoint

**Test:** Kill job mid-training, restart, verify it resumes.

**Files to create:**
- `aethergrid/checkpointing/manager.py`

### Step 4.2: Dataset Mounting

- [ ] Support mounting host directories into container
- [ ] Support S3/GCS mounting (via fuse or s3fs)
- [ ] Task spec includes volume mounts

**Test:** Submit task that reads from mounted dataset.

**Files to modify:**
- `aethergrid/worker/agent.py`
- `aethergrid/core/state_machine.py`

### Step 4.3: Multi-Node Training (MPI/NCCL)

- [ ] Define "distributed job" as multiple coordinated tasks
- [ ] Leader assigns all tasks simultaneously
- [ ] Workers set up NCCL/MPICH communication
- [ ] Track job-level status (not just task-level)

**Test:** Run simple distributed training (e.g., PyTorch DDP).

**Files to create:**
- `aethergrid/distributed/job.py`
- `aethergrid/distributed/launcher.py`

---

## Phase 5: Production Hardening

Make it reliable enough for real use.

### Step 5.1: Fault Recovery

- [ ] On worker failure, reschedule tasks to other workers
- [ ] Preserve checkpoints across rescheduling
- [ ] Handle partial failures in distributed jobs

**Test:** Kill worker mid-job, verify task reschedules.

### Step 5.2: Observability

- [ ] Add structured logging
- [ ] Add Prometheus metrics for job queue depth, GPU utilization
- [ ] Add tracing for job lifecycle

**Test:** View metrics in Grafana.

### Step 5.3: CLI Tooling

- [ ] `aether submit job.yaml` - Submit job from file
- [ ] `aether list jobs` - List jobs
- [ ] `aether logs <job-id>` - Stream logs
- [ ] `aether cancel <job-id>` - Cancel job

**Test:** Manage jobs entirely via CLI.

---

## Quick Start: What to Build First

**Start with Step 0.1** - Run local execution test.

This validates the core execution model works before you invest in networking. If this fails, fix the execution model first.

**Do this:**
```bash
python3 verify_local_execution.py
```

If you see `RESULT: SUCCESS`, proceed to Step 1.1 (wire up gRPC server).

If you see `RESULT: FAILED` or `RESULT: TIMEOUT`, debug the worker agent before moving on.

---

## What NOT to Build (Yet)

Don't waste time on these until you have the basics working:

- Multi-Raft sharding (single Raft is fine for <10K jobs)
- Web UI (CLI is enough)
- Advanced scheduling algorithms (round-robin works)
- Authentication (add later)
- Multi-region deployment (single cluster first)
- Auto-scaling workers (manual scaling first)

---

## Success Metrics

After each phase, you should be able to:

| Phase | Success Metric |
|-------|----------------|
| 0 | Run mock training job locally, see SUCCESS |
| 1 | 3 nodes communicating, leader election works |
| 2 | Submit job, it runs on a worker, get logs |
| 3 | Submit GPU job, it uses GPU |
| 4 | Run distributed training across 2+ nodes |
| 5 | Kill a worker, jobs recover automatically |

---

## File Structure (Target)

```
aethergrid/
├── api/                    # REST API endpoints
│   ├── health.py          # Health checks
│   └── tasks.py           # Task submission/query
├── checkpointing/          # Job checkpointing
│   └── manager.py
├── cli.py                  # CLI commands
├── core/                   # Core Raft + state machine (keep as-is)
│   ├── raft_node.py
│   └── state_machine.py
├── distributed/            # Multi-node training
│   ├── job.py
│   └── launcher.py
├── examples/               # Example scripts
│   └── train_mock.py      # Mock training script
├── grpc_services.py        # gRPC service impl (create this)
├── storage/                # Storage backend (keep as-is)
│   └── backend.py
└── worker/                 # Worker agent (enhance)
    └── agent.py
```

---

## Future Phase: Enterprise AI Compute

The following features are planned for future releases to support enterprise-scale AI workloads:

### Multi-Region Scheduling

**Goal:** Distribute training jobs across multiple data centers / cloud regions.

**Key Features:**
- Region-aware task placement with latency optimization
- Cross-region state replication for disaster recovery
- Federated learning support with region-local data
- Cost-aware scheduling (spot instances, reserved capacity)
- Network topology awareness for distributed training

**Implementation:**
```
Region A (us-west)        Region B (us-east)        Region C (eu-west)
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  Raft Leader    │◄────►│  Raft Follower  │◄────►│  Raft Follower  │
│  + GPU Workers  │      │  + GPU Workers  │      │  + GPU Workers  │
└─────────────────┘      └─────────────────┘      └─────────────────┘
         │                       │                       │
         └───────────────────────┴───────────────────────┘
                    Async State Replication
```

**Proto Changes:**
```protobuf
message RegionSpec {
    string region_id = 1;
    string zone = 2;
    double cost_per_gpu_hour = 3;
    int32 latency_ms_to_region = 4;
}

message PlacementConstraints {
    // ... existing fields ...
    repeated string preferred_regions = 10;
    repeated string anti_affinity_regions = 11;
    bool require_same_region = 12;  // For distributed training
}
```

---

### Kubernetes Operator Integration

**Goal:** Native Kubernetes deployment with CRDs for AetherGrid resources.

**Key Features:**
- `AetherTask` Custom Resource Definition for declarative job submission
- `AetherWorker` CRD for worker node management
- `AetherCluster` CRD for multi-node cluster deployment
- Automatic horizontal scaling based on queue depth
- Integration with Kubernetes GPU scheduling (device plugins)
- Helm charts for one-command deployment

**Example CRD:**
```yaml
apiVersion: aethergrid.io/v1
kind: AetherTask
metadata:
  name: gpt4-finetune
  namespace: ml-training
spec:
  image: pytorch/pytorch:2.0-cuda11.8
  command: ["python", "train.py"]
  resources:
    gpuCount: 8
    gpuMemoryMB: 80000  # 80GB A100s
    cpuMillis: 32000
    memoryBytes: 256Gi
  priority: 100
  checkpointInterval: 1000  # steps
  maxRetries: 3
  placement:
    requiredLabels:
      - "accelerator=a100"
    preferredRegions:
      - "us-west-2"
```

**Operator Architecture:**
```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              AetherGrid Operator                      │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │
│  │  │ Task Ctrl   │  │Worker Ctrl  │  │Cluster Ctrl │   │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘   │   │
│  └──────────────────────────────────────────────────────┘   │
│                          │                                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │                 Raft Leader Pod                       │   │
│  │  ┌─────────────────────────────────────────────────┐ │   │
│  │  │ State Machine | Scheduler | gRPC Services       │ │   │
│  │  └─────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────┘   │
│                          │                                   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐        │
│  │ Worker 1 │ │ Worker 2 │ │ Worker 3 │ │ Worker N │        │
│  │ (GPU)    │ │ (GPU)    │ │ (CPU)    │ │ (GPU)    │        │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘        │
└─────────────────────────────────────────────────────────────┘
```

---

### Automatic GPU De-fragmentation

**Goal:** Maximize GPU utilization by consolidating workloads and defragmenting GPU memory.

**Problem:**
```
Before De-fragmentation:
Node A: [GPU0: 70% used] [GPU1: 30% used] [GPU2: 50% used] [GPU3: 10% used]
Node B: [GPU0: 20% used] [GPU1: 80% used] [GPU2: 40% used] [GPU3: 60% used]

Result: Cannot schedule 8-GPU job (fragmented across nodes)
```

**Solution:**
```
After De-fragmentation:
Node A: [GPU0: 100% used] [GPU1: 100% used] [GPU2: 100% used] [GPU3: 100% used]
Node B: [GPU0: 0% used] [GPU1: 0% used] [GPU2: 0% used] [GPU3: 0% used]

Result: 8-GPU job can now be scheduled on Node B
```

**Key Features:**
- Live migration of GPU workloads (checkpoint + restore)
- Predictive de-fragmentation based on job queue analysis
- Cost-aware migration (minimize data transfer)
- Multi-GPU job co-location optimization
- Memory-aware scheduling for large models

**Implementation:**
```python
class GPUScheduler:
    def calculate_fragmentation_score(self, node: Node) -> float:
        """Calculate GPU fragmentation score (0 = compact, 1 = fragmented)."""
        if node.capacity.gpu_count == 0:
            return 0.0
        
        utilization = []
        for gpu_id in range(node.capacity.gpu_count):
            used = node.allocated_gpu_memory.get(gpu_id, 0)
            total = node.capacity.gpu_memory_mb
            utilization.append(used / total)
        
        # Fragmentation = variance in utilization
        mean_util = sum(utilization) / len(utilization)
        variance = sum((u - mean_util) ** 2 for u in utilization) / len(utilization)
        return variance
    
    def find_migration_candidates(self) -> List[Tuple[Task, Node]]:
        """Find tasks that could be migrated to reduce fragmentation."""
        candidates = []
        for node in self.state_machine.nodes.values():
            if self.calculate_fragmentation_score(node) > 0.3:
                # Find small tasks that could be consolidated
                for task_id in node.running_tasks:
                    task = self.state_machine.tasks.get(task_id)
                    if task and task.resources.gpu_count <= 2:
                        candidates.append((task, node))
        return candidates
    
    def plan_defragmentation(self) -> List[MigrationPlan]:
        """Create a migration plan to reduce cluster fragmentation."""
        plans = []
        candidates = self.find_migration_candidates()
        
        # Group small tasks together
        for task, source_node in candidates:
            target_node = self.find_consolidation_target(task, source_node)
            if target_node:
                plans.append(MigrationPlan(
                    task=task,
                    source=source_node,
                    target=target_node,
                    estimated_downtime=task.checkpoint_index * 0.001,  # seconds
                ))
        
        return plans
```

**Proto Changes:**
```protobuf
message DefragmentationPolicy {
    bool enabled = 1;
    float fragmentation_threshold = 2;  // Trigger at 30% fragmentation
    int32 min_task_age_seconds = 3;     // Don't migrate new tasks
    bool allow_preemption = 4;          // Preempt low-priority tasks
    int32 max_concurrent_migrations = 5;
}

message MigrationPlan {
    TaskID task_id = 1;
    NodeID source_node = 2;
    NodeID target_node = 3;
    uint64 estimated_downtime_ms = 4;
    string checkpoint_url = 5;
}
```

---

## Feature Status Summary

| Feature | Status | Phase |
|---------|--------|-------|
| Docker execution | ✅ Complete | Phase 2 |
| GPU resource tracking | ✅ Complete | Phase 3 |
| Best-fit scheduling | ✅ Complete | Phase 3 |
| Resource reclamation | ✅ Complete | Phase 3 |
| Task priority | ✅ Complete | Phase 3 |
| Task preemption | ✅ Complete | Phase 3 |
| AI checkpointing | ✅ Complete | Phase 4 |
| Cluster dashboard | ✅ Complete | Phase 4 |
| Multi-node training | 🔄 Planned | Phase 4 |
| Fault recovery | 🔄 Planned | Phase 5 |
| Multi-region scheduling | 📋 Future | Phase 6 |
| Kubernetes operator | 📋 Future | Phase 6 |
| GPU de-fragmentation | 📋 Future | Phase 6 |
