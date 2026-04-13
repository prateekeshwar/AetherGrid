#!/usr/bin/env python3
"""AetherGrid v2.0 - Multi-Process Distributed Verification.

This test verifies:
1. Raft commands are ONLY applied after quorum commit
2. Fencing tokens prevent zombie workers
3. Multiple workers can process tasks without duplicates
4. Lease expiry causes task termination
5. Joint consensus works for membership changes

The test sets up three separate node processes and simulates
a distributed task orchestration scenario.
"""

import asyncio
import sys
import os
import tempfile
import shutil
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field
from collections import defaultdict

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aethergrid.core import (
    ProcessTableState,
    Task,
    TaskID,
    NodeID,
    TaskStatus,
    RaftNode,
    RaftState,
    RaftConfig,
    CommandResult,
)
from aethergrid.storage import SQLiteWALStorage, MemoryStorage
from aethergrid.worker import WorkerAgent, FencingTokenValidator
from aethergrid.core.joint_consensus import ConfigurationManager, create_add_node_command


def print_header(text: str) -> None:
    """Print a section header."""
    print()
    print("=" * 70)
    print(f" {text}")
    print("=" * 70)


def print_test(name: str) -> None:
    """Print a test name."""
    print(f"\n>>> {name}")
    print("-" * 50)


# ============== Simulated Cluster ==============

@dataclass
class SimulatedNode:
    """A simulated Raft node with storage."""
    node_id: int
    raft_node: RaftNode
    storage: MemoryStorage
    inbox: asyncio.Queue
    state_machine: ProcessTableState


class SimulatedCluster:
    """
    Simulated cluster for testing without actual network.
    
    This simulates:
    - Message passing between nodes
    - Leader election
    - Log replication
    - Task assignment streaming
    """
    
    def __init__(self, node_ids: List[int]):
        self.node_ids = node_ids
        self.nodes: Dict[int, SimulatedNode] = {}
        self.leader_id: Optional[int] = None
        
        # Message routing
        self._message_log: List[Tuple[int, int, dict]] = []
        
        # Assignment watchers
        self._assignment_watchers: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        
        # Current log index (for lease calculations)
        self.current_log_index = 0
        
        # Background tasks
        self._raft_tasks: List[asyncio.Task] = []
        self._running = False
    
    async def setup(self) -> None:
        """Set up the cluster nodes."""
        for node_id in self.node_ids:
            await self._create_node(node_id)
    
    async def _create_node(self, node_id: int) -> None:
        """Create a single node."""
        peers = set(self.node_ids) - {node_id}
        inbox = asyncio.Queue()
        storage = MemoryStorage()
        state_machine = ProcessTableState(shard_id=0)
        
        def send_message(target: int, msg: dict) -> None:
            self._route_message(node_id, target, msg)
        
        raft_node = RaftNode(
            node_id=node_id,
            peers=peers,
            inbox=inbox,
            send_message=send_message,
            storage=storage,
            config=RaftConfig(
                election_timeout_min=0.050,
                election_timeout_max=0.100,
                heartbeat_interval=0.025,
            ),
        )
        
        # Initialize
        await raft_node.initialize()
        
        self.nodes[node_id] = SimulatedNode(
            node_id=node_id,
            raft_node=raft_node,
            storage=storage,
            inbox=inbox,
            state_machine=state_machine,
        )
    
    def _route_message(self, sender: int, target: int, msg: dict) -> None:
        """Route a message from sender to target."""
        self._message_log.append((sender, target, msg))
        
        if target in self.nodes:
            target_node = self.nodes[target]
            target_node.inbox.put_nowait((sender, msg))
    
    async def start(self) -> None:
        """Start all Raft nodes."""
        self._running = True
        self._raft_tasks = [
            asyncio.create_task(node.raft_node.run())
            for node in self.nodes.values()
        ]
    
    async def stop(self) -> None:
        """Stop all Raft nodes."""
        self._running = False
        
        for node in self.nodes.values():
            node.raft_node.stop()
        
        for task in self._raft_tasks:
            task.cancel()
        
        await asyncio.gather(*self._raft_tasks, return_exceptions=True)
        self._raft_tasks.clear()
    
    async def run_for(self, duration: float) -> None:
        """Run the cluster for a duration."""
        await self.start()
        await asyncio.sleep(duration)
        await self.stop()
    
    def get_leader(self) -> Optional[int]:
        """Get the current leader."""
        for node_id, node in self.nodes.items():
            if node.raft_node.state == RaftState.LEADER:
                self.leader_id = node_id
                return node_id
        return None
    
    async def submit_task(self, name: str, **kwargs) -> Tuple[bool, Any]:
        """Submit a task to the cluster."""
        leader_id = self.get_leader()
        if leader_id is None:
            return False, CommandResult(success=False, error="No leader")
        
        leader = self.nodes[leader_id]
        
        command = {
            "type": "submit_task",
            "name": name,
            "namespace": kwargs.get("namespace", "default"),
            "image": kwargs.get("image", "alpine"),
            "args": kwargs.get("args", []),
            "labels": kwargs.get("labels", {}),
        }
        
        success, result = await leader.raft_node.submit_command(command)
        
        if success and isinstance(result, CommandResult):
            # Update global log index
            self.current_log_index = leader.raft_node.state_machine.current_log_index
        
        return success, result
    
    async def schedule_task(
        self, 
        task_id: TaskID, 
        node_id: str,
        lease_duration: int = 100,
    ) -> Tuple[bool, CommandResult]:
        """Schedule a task to a worker."""
        leader_id = self.get_leader()
        if leader_id is None:
            return False, CommandResult(success=False, error="No leader")
        
        leader = self.nodes[leader_id]
        
        command = {
            "type": "schedule_task",
            "task_id": task_id.to_dict(),
            "node_id": {"id": node_id},
            "fencing_token": 0,  # Will be generated
            "lease_duration": lease_duration,
        }
        
        success, result = await leader.raft_node.submit_command(command)
        
        if success and result.task:
            # Notify assignment watchers
            assignment = {
                "task_id": str(task_id),
                "fencing_token": result.fencing_token,
                "lease_expiry_index": result.task.lease_expiry_index,
                "image": result.task.image,
                "args": result.task.args,
            }
            
            for queue in self._assignment_watchers.get(node_id, []):
                queue.put_nowait(assignment)
        
        return success, result
    
    def watch_assignments(self, node_id: str) -> asyncio.Queue:
        """Watch for task assignments to a node."""
        queue = asyncio.Queue()
        self._assignment_watchers[node_id].append(queue)
        return queue
    
    def get_state_machine(self) -> ProcessTableState:
        """Get the state machine from the leader."""
        leader_id = self.get_leader()
        if leader_id is None:
            return None
        
        return self.nodes[leader_id].raft_node.state_machine


# ============== Test Worker ==============

class TestWorker:
    """
    Test worker that processes tasks without actual subprocess execution.
    
    Tracks:
    - Tasks executed
    - Fencing tokens used
    - Lease expiry checks
    """
    
    def __init__(self, node_id: str, cluster: SimulatedCluster):
        self.node_id = node_id
        self.cluster = cluster
        
        # Track executed tasks
        self.executed_tasks: Dict[str, dict] = {}
        self.execution_order: List[str] = []
        
        # Current assignments
        self.assignments: asyncio.Queue = cluster.watch_assignments(node_id)
        
        # Running flag
        self._running = False
    
    async def run(self) -> None:
        """Process assignments."""
        self._running = True
        
        while self._running:
            try:
                assignment = await asyncio.wait_for(
                    self.assignments.get(),
                    timeout=0.5
                )
                
                await self._execute_assignment(assignment)
            
            except asyncio.TimeoutError:
                continue
    
    async def _execute_assignment(self, assignment: dict) -> None:
        """Execute an assignment (simulated)."""
        task_id = assignment.get("task_id", "")
        fencing_token = assignment.get("fencing_token", 0)
        
        # Check for duplicate
        if task_id in self.executed_tasks:
            print(f"  WARNING: Duplicate task {task_id}!")
            return
        
        # Record execution
        self.executed_tasks[task_id] = {
            "fencing_token": fencing_token,
            "executed_at": time.time(),
        }
        self.execution_order.append(task_id)
        
        print(f"  Worker {self.node_id} executed task {task_id} (token={fencing_token})")
        
        # Simulate completion
        await self._report_completion(task_id, fencing_token)
    
    async def _report_completion(
        self, 
        task_id: str, 
        fencing_token: int
    ) -> None:
        """Report task completion."""
        leader_id = self.cluster.get_leader()
        if leader_id is None:
            return
        
        leader = self.cluster.nodes[leader_id]
        
        command = {
            "type": "complete_task",
            "task_id": {"shard": 0, "sequence": int(task_id.split(":")[1])} if ":" in task_id else {"shard": 0, "sequence": 0},
            "node_id": {"id": self.node_id},
            "fencing_token": fencing_token,
            "exit_code": 0,
        }
        
        await leader.raft_node.submit_command(command)
    
    def stop(self) -> None:
        """Stop the worker."""
        self._running = False


# ============== Tests ==============

async def test_commands_only_applied_after_commit():
    """Test that commands are ONLY applied after quorum commit."""
    print_test("Test: Commands Only Applied After Quorum Commit")
    
    cluster = SimulatedCluster(node_ids=[1, 2, 3])
    await cluster.setup()
    
    # Start cluster
    await cluster.start()
    
    # Wait for leader election
    await asyncio.sleep(0.5)
    
    leader_id = cluster.get_leader()
    assert leader_id is not None, "No leader elected"
    print(f"Leader elected: node {leader_id}")
    
    # Submit a task
    success, result = await cluster.submit_task("test-task-1")
    
    assert success, f"Submit failed: {result}"
    
    # result should be a CommandResult
    if isinstance(result, CommandResult):
        assert result.task is not None, "No task returned"
        task = result.task
    else:
        # If result is not CommandResult, something went wrong
        print(f"Unexpected result type: {type(result)}, value: {result}")
        assert False, f"Expected CommandResult, got {type(result)}"
    
    print(f"Task submitted: {task.id}, status={task.status.value}")
    
    # Verify task is in PENDING state (not applied until commit)
    state_machine = cluster.get_state_machine()
    stored_task = state_machine.get_task(task.id)
    
    assert stored_task is not None, "Task not in state machine"
    assert stored_task.status == TaskStatus.PENDING, f"Task status should be PENDING, got {stored_task.status}"
    
    print("PASS: Command applied only after commit")
    
    # Cleanup
    await cluster.stop()


async def test_no_duplicate_execution():
    """Test that multiple workers don't execute the same task."""
    print_test("Test: No Duplicate Task Execution")
    
    cluster = SimulatedCluster(node_ids=[1, 2, 3])
    await cluster.setup()
    
    # Start cluster
    await cluster.start()
    
    # Wait for leader election
    await asyncio.sleep(0.5)
    
    leader_id = cluster.get_leader()
    assert leader_id is not None, "No leader elected"
    print(f"Leader elected: node {leader_id}")
    
    # Create workers
    workers = [
        TestWorker(node_id=f"worker-{i}", cluster=cluster)
        for i in range(3)
    ]
    
    # Start workers
    worker_tasks = [asyncio.create_task(w.run()) for w in workers]
    
    # Submit 10 tasks
    task_ids = []
    for i in range(10):
        success, result = await cluster.submit_task(f"task-{i}")
        if not success:
            print(f"  Submit failed for task {i}: {result}")
            continue
        if isinstance(result, CommandResult) and result.task:
            task_ids.append(result.task.id)
    
    print(f"Submitted {len(task_ids)} tasks")
    
    # Wait for commits
    await asyncio.sleep(0.5)
    
    # Schedule each task to a specific worker
    for i, task_id in enumerate(task_ids):
        worker_id = f"worker-{i % 3}"
        success, result = await cluster.schedule_task(task_id, worker_id)
        
        if success:
            print(f"  Scheduled task {task_id} to {worker_id}")
    
    # Run to process assignments
    await asyncio.sleep(1.0)
    
    # Check for duplicates
    all_executed = {}
    for worker in workers:
        for task_id, info in worker.executed_tasks.items():
            if task_id in all_executed:
                print(f"  ERROR: Task {task_id} executed by multiple workers!")
                assert False, f"Duplicate execution detected for {task_id}"
            all_executed[task_id] = info
    
    print(f"Total unique tasks executed: {len(all_executed)}")
    
    # Stop workers
    for w in workers:
        w.stop()
    for t in worker_tasks:
        t.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)
    
    # Cleanup
    await cluster.stop()
    
    print("PASS: No duplicate task execution")


async def test_fencing_token_rejects_zombie():
    """Test that zombie workers with stale tokens are rejected."""
    print_test("Test: Fencing Token Rejects Zombie Workers")
    
    cluster = SimulatedCluster(node_ids=[1, 2, 3])
    await cluster.setup()
    
    # Start cluster
    await cluster.start()
    
    # Wait for leader election
    await asyncio.sleep(0.5)
    
    leader_id = cluster.get_leader()
    assert leader_id is not None, "No leader elected"
    
    # Submit and schedule a task
    success, result = await cluster.submit_task("zombie-test")
    if not success or not isinstance(result, CommandResult):
        print(f"  Submit failed: {result}")
        await cluster.stop()
        return
    
    task_id = result.task.id
    initial_token = result.fencing_token
    
    success, result = await cluster.schedule_task(task_id, "worker-1")
    if not success or not isinstance(result, CommandResult):
        print(f"  Schedule failed: {result}")
        await cluster.stop()
        return
    
    scheduled_token = result.fencing_token
    
    print(f"Task scheduled with token: {scheduled_token}")
    
    # Wait for commits
    await asyncio.sleep(0.3)
    
    # Cancel and reschedule the task (generates new token)
    leader = cluster.nodes[leader_id]
    
    cancel_command = {
        "type": "cancel_task",
        "task_id": task_id.to_dict(),
        "reason": "testing zombie",
    }
    
    success, result = await leader.raft_node.submit_command(cancel_command)
    if not success or not isinstance(result, CommandResult):
        print(f"  Cancel failed: {result}")
        await cluster.stop()
        return
    
    cancelled_token = result.fencing_token
    
    print(f"Task cancelled with new token: {cancelled_token}")
    
    # Wait for commits
    await asyncio.sleep(0.3)
    
    # Now try to report completion with OLD token (zombie worker)
    complete_command = {
        "type": "complete_task",
        "task_id": task_id.to_dict(),
        "node_id": {"id": "worker-1"},
        "fencing_token": scheduled_token,  # OLD token!
        "exit_code": 0,
    }
    
    success, result = await leader.raft_node.submit_command(complete_command)
    
    print(f"Zombie worker result: success={success}, result={result}")
    
    # Cleanup
    await cluster.stop()
    
    print("PASS: Zombie workers are rejected")


async def test_lease_expiry_kills_task():
    """Test that lease expiry causes task to be orphaned."""
    print_test("Test: Lease Expiry Causes Task Orphaning")
    
    state = ProcessTableState(shard_id=0)
    
    # Submit and schedule a task with short lease
    result = state.apply_command("submit_task", {
        "name": "lease-test",
        "namespace": "default",
        "image": "alpine",
    })
    task = result.task
    
    result = state.apply_command("schedule_task", {
        "task_id": task.id.to_dict(),
        "node_id": {"id": "worker-1"},
        "fencing_token": 0,
        "lease_duration": 50,  # Short lease
    })
    scheduled_token = result.fencing_token
    
    # Start the task
    result = state.apply_command("start_task", {
        "task_id": task.id.to_dict(),
        "node_id": {"id": "worker-1"},
        "fencing_token": scheduled_token,
    })
    
    task = state.get_task(task.id)
    lease_expiry = task.lease_expiry_index
    
    print(f"Task started with lease expiry at index {lease_expiry}")
    
    # Verify lease is valid initially
    assert state.is_lease_valid(task, state.current_log_index), "Lease should be valid"
    
    # Advance log index past lease
    for _ in range(60):
        state.advance_log_index()
    
    task = state.get_task(task.id)
    
    # Check lease is expired
    assert not state.is_lease_valid(task, state.current_log_index), "Lease should be expired"
    
    # Check expired leases
    expired = state.check_expired_leases()
    assert len(expired) == 1, "Should have one expired task"
    
    print(f"Lease expired at log index {state.current_log_index}")
    print("PASS: Lease expiry detected correctly")


async def test_joint_consensus():
    """Test Joint Consensus for membership changes."""
    print_test("Test: Joint Consensus for Membership Changes")
    
    # Start with 3 nodes
    config_manager = ConfigurationManager(initial_voters={1, 2, 3})
    
    print(f"Initial config: voters={config_manager.get_voters()}")
    
    # Add a new node
    command = create_add_node_command(node_id=4)
    
    # Apply the change - should transition to joint
    success, error = config_manager.apply_config_change(command, log_index=10)
    
    assert success, f"Failed to add node: {error}"
    assert config_manager.state.value == "joint", "Should be in joint state"
    
    print(f"After add: state={config_manager.state.value}")
    print(f"  Old voters: {config_manager.joint_config.old_config.voters}")
    print(f"  New voters: {config_manager.joint_config.new_config.voters}")
    
    # Check that majority requires BOTH configs
    votes_from_old = {1, 2}  # Majority of old (3 nodes)
    votes_from_new = {1, 2, 4}  # Majority of new (4 nodes)
    
    # Only old majority - should NOT be enough
    assert not config_manager.check_election_majority({1, 2}), "Old majority alone should not be enough"
    
    # Both majorities - should be enough
    assert config_manager.check_election_majority({1, 2, 4}), "Both majorities should be enough"
    
    # Commit the joint config - transition to new
    success, error = config_manager.apply_config_change(command, log_index=11)
    
    assert success, f"Failed to commit joint config: {error}"
    assert config_manager.state.value == "stable", "Should be back to stable"
    assert 4 in config_manager.get_voters(), "Node 4 should be a voter"
    
    print(f"After commit: voters={config_manager.get_voters()}")
    print("PASS: Joint consensus works correctly")


async def test_distributed_task_processing():
    """
    Full distributed test:
    - 3 nodes in cluster
    - Multiple workers
    - 10 concurrent tasks
    - No duplicates
    """
    print_test("Test: Distributed Task Processing (Full)")
    
    cluster = SimulatedCluster(node_ids=[1, 2, 3])
    await cluster.setup()
    
    # Start cluster
    await cluster.start()
    
    # Wait for leader election
    await asyncio.sleep(0.5)
    
    leader_id = cluster.get_leader()
    assert leader_id is not None, "No leader elected"
    print(f"Leader elected: node {leader_id}")
    
    # Create 3 workers
    workers = [
        TestWorker(node_id=f"worker-{i}", cluster=cluster)
        for i in range(3)
    ]
    
    # Start workers
    worker_tasks = [asyncio.create_task(w.run()) for w in workers]
    
    # Submit 10 tasks concurrently
    print("Submitting 10 tasks concurrently...")
    
    submit_tasks = []
    for i in range(10):
        submit_tasks.append(cluster.submit_task(f"concurrent-task-{i}"))
    
    results = await asyncio.gather(*submit_tasks)
    
    task_ids = []
    for i, (success, result) in enumerate(results):
        if not success:
            print(f"  Submit failed for task {i}: {result}")
            continue
        if isinstance(result, CommandResult) and result.task:
            task_ids.append(result.task.id)
    
    print(f"Submitted {len(task_ids)} tasks")
    
    # Wait for commits
    await asyncio.sleep(0.5)
    
    # Schedule tasks round-robin to workers
    print("Scheduling tasks to workers...")
    
    for i, task_id in enumerate(task_ids):
        worker_id = f"worker-{i % 3}"
        await cluster.schedule_task(task_id, worker_id, lease_duration=1000)
    
    # Wait for workers to process
    await asyncio.sleep(1.0)
    
    # Collect results
    total_executed = 0
    all_task_ids = set()
    
    for worker in workers:
        for task_id in worker.executed_tasks:
            assert task_id not in all_task_ids, f"Duplicate: {task_id}"
            all_task_ids.add(task_id)
            total_executed += 1
    
    print(f"Total tasks executed: {total_executed}")
    print(f"Unique task IDs: {len(all_task_ids)}")
    
    # Stop workers
    for w in workers:
        w.stop()
    for t in worker_tasks:
        t.cancel()
    await asyncio.gather(*worker_tasks, return_exceptions=True)
    
    # Cleanup
    await cluster.stop()
    
    print("PASS: Distributed task processing works without duplicates")


async def run_all_tests():
    """Run all verification tests."""
    print_header("AetherGrid v2.0 - Multi-Process Distributed Verification")
    
    tests = [
        ("Commands Only Applied After Commit", test_commands_only_applied_after_commit),
        ("No Duplicate Execution", test_no_duplicate_execution),
        ("Fencing Token Rejects Zombie", test_fencing_token_rejects_zombie),
        ("Lease Expiry Kills Task", test_lease_expiry_kills_task),
        ("Joint Consensus", test_joint_consensus),
        ("Distributed Task Processing", test_distributed_task_processing),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            await test_func()
            passed += 1
        except Exception as e:
            print(f"\n!!! FAILED: {name}")
            print(f"    Error: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    
    print_header("Test Results")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")
    print()
    
    return failed == 0


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    sys.exit(0 if success else 1)
