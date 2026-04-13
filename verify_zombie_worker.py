#!/usr/bin/env python3
"""AetherGrid v2.0 - Zombie Worker Protection Verification.

This test verifies that the fencing token mechanism correctly prevents
zombie workers from corrupting task state.

Scenarios tested:
1. Normal task lifecycle with valid fencing tokens
2. Zombie worker (stale token) cannot update task state
3. Task reassignment generates new fencing token
4. Log-based lease expiration
5. Deterministic time (log index) instead of wall clock
"""

import asyncio
import sys
import os
import tempfile
import shutil

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aethergrid.core import (
    ProcessTableState,
    Task,
    TaskID,
    NodeID,
    TaskStatus,
    CommandResult,
)
from aethergrid.storage import SQLiteWALStorage, MemoryStorage
from aethergrid.worker import FencingTokenValidator


def print_header(text: str) -> None:
    """Print a section header."""
    print()
    print("=" * 60)
    print(f" {text}")
    print("=" * 60)


def print_test(name: str) -> None:
    """Print a test name."""
    print(f"\n>>> {name}")
    print("-" * 40)


def test_fencing_token_generation():
    """Test that fencing tokens are monotonically increasing."""
    print_test("Test: Fencing Token Generation")
    
    state = ProcessTableState(shard_id=0)
    
    tokens = []
    for i in range(10):
        token = state.generate_fencing_token()
        tokens.append(token)
    
    # Verify tokens are increasing
    for i in range(1, len(tokens)):
        assert tokens[i] > tokens[i-1], f"Token {tokens[i]} not > {tokens[i-1]}"
    
    print(f"Generated tokens: {tokens}")
    print("PASS: Tokens are monotonically increasing")


def test_valid_fencing_token():
    """Test that valid fencing tokens are accepted."""
    print_test("Test: Valid Fencing Token")
    
    state = ProcessTableState(shard_id=0)
    
    # Submit a task
    result = state.apply_command("submit_task", {
        "name": "test-task",
        "namespace": "default",
        "image": "alpine",
    })
    
    assert result.success, f"Submit failed: {result.error}"
    task = result.task
    initial_token = result.fencing_token
    
    print(f"Task submitted with fencing token: {initial_token}")
    
    # Schedule the task with valid token
    result = state.apply_command("schedule_task", {
        "task_id": task.id.to_dict(),
        "node_id": {"id": "worker-1"},
        "fencing_token": 0,  # Not needed for schedule (generates new token)
        "lease_duration": 100,
    })
    
    assert result.success, f"Schedule failed: {result.error}"
    new_token = result.fencing_token
    
    print(f"Task scheduled with new fencing token: {new_token}")
    assert new_token > initial_token, "New token should be > initial token"
    
    print("PASS: Valid fencing tokens accepted")


def test_zombie_worker_rejected():
    """Test that zombie workers with stale tokens are rejected."""
    print_test("Test: Zombie Worker Rejection")
    
    state = ProcessTableState(shard_id=0)
    
    # Submit and schedule a task
    result = state.apply_command("submit_task", {
        "name": "zombie-test",
        "namespace": "default",
        "image": "alpine",
    })
    task = result.task
    original_token = result.fencing_token
    
    result = state.apply_command("schedule_task", {
        "task_id": task.id.to_dict(),
        "node_id": {"id": "worker-1"},
        "fencing_token": 0,
        "lease_duration": 100,
    })
    scheduled_token = result.fencing_token
    
    print(f"Task scheduled with token: {scheduled_token}")
    
    # Simulate: Task is cancelled and reassigned (new token generated)
    result = state.apply_command("cancel_task", {
        "task_id": task.id.to_dict(),
        "reason": "reassigning",
    })
    
    cancelled_token = result.fencing_token
    print(f"Task cancelled with new token: {cancelled_token}")
    
    # Now a "zombie worker" tries to report completion with OLD token
    print("\nZombie worker attempting to report completion...")
    
    result = state.apply_command("complete_task", {
        "task_id": task.id.to_dict(),
        "node_id": {"id": "worker-1"},
        "fencing_token": scheduled_token,  # OLD token!
        "exit_code": 0,
    })
    
    assert not result.success, "Zombie worker should be rejected!"
    assert "Stale fencing token" in result.error, f"Wrong error: {result.error}"
    
    print(f"Zombie worker correctly rejected: {result.error}")
    print("PASS: Zombie worker protection works!")


def test_task_reassignment_invalidation():
    """Test that task reassignment invalidates old tokens."""
    print_test("Test: Task Reassignment Invalidation")
    
    state = ProcessTableState(shard_id=0)
    
    # Submit a task
    result = state.apply_command("submit_task", {
        "name": "reassign-test",
        "namespace": "default",
        "image": "alpine",
    })
    task = result.task
    
    # Schedule to worker-1
    result = state.apply_command("schedule_task", {
        "task_id": task.id.to_dict(),
        "node_id": {"id": "worker-1"},
        "fencing_token": 0,
        "lease_duration": 100,
    })
    token_v1 = result.fencing_token
    print(f"Task scheduled to worker-1 with token: {token_v1}")
    
    # Worker-1 starts the task
    result = state.apply_command("start_task", {
        "task_id": task.id.to_dict(),
        "node_id": {"id": "worker-1"},
        "fencing_token": token_v1,
    })
    assert result.success, f"Start failed: {result.error}"
    
    # Simulate: worker-1 dies, task is orphaned
    result = state.apply_command("orphan_task", {
        "task_id": task.id.to_dict(),
        "dead_node": {"id": "worker-1"},
    })
    token_v2 = result.fencing_token
    print(f"Task orphaned, new token: {token_v2}")
    
    # Task is rescheduled to worker-2
    result = state.apply_command("schedule_task", {
        "task_id": task.id.to_dict(),
        "node_id": {"id": "worker-2"},
        "fencing_token": 0,
        "lease_duration": 100,
    })
    token_v3 = result.fencing_token
    print(f"Task rescheduled to worker-2 with token: {token_v3}")
    
    # Worker-2 starts the task
    result = state.apply_command("start_task", {
        "task_id": task.id.to_dict(),
        "node_id": {"id": "worker-2"},
        "fencing_token": token_v3,
    })
    assert result.success, f"Start failed: {result.error}"
    
    # Now worker-1 (zombie) tries to complete with old token
    print("\nZombie worker-1 attempting to complete with old token...")
    result = state.apply_command("complete_task", {
        "task_id": task.id.to_dict(),
        "node_id": {"id": "worker-1"},
        "fencing_token": token_v1,  # OLD token from before orphan
        "exit_code": 0,
    })
    
    assert not result.success, "Zombie worker should be rejected!"
    print(f"Zombie worker rejected: {result.error}")
    
    # Worker-2 completes with valid token
    print("\nLegitimate worker-2 completing with valid token...")
    result = state.apply_command("complete_task", {
        "task_id": task.id.to_dict(),
        "node_id": {"id": "worker-2"},
        "fencing_token": token_v3,  # Current valid token
        "exit_code": 0,
    })
    
    assert result.success, f"Valid completion failed: {result.error}"
    print("PASS: Task reassignment correctly invalidates old tokens")


def test_log_based_lease():
    """Test that leases expire based on log index (deterministic time)."""
    print_test("Test: Log-Based Lease Expiration")
    
    state = ProcessTableState(shard_id=0)
    
    # Submit and schedule a task
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
        "lease_duration": 100,  # 100 log entries
    })
    scheduled_token = result.fencing_token
    
    # Start the task
    result = state.apply_command("start_task", {
        "task_id": task.id.to_dict(),
        "node_id": {"id": "worker-1"},
        "fencing_token": scheduled_token,
    })
    
    # Get the task to check lease
    task = state.get_task(task.id)
    lease_start = state.current_log_index
    lease_expiry = task.lease_expiry_index
    
    print(f"Lease start index: {lease_start}")
    print(f"Lease expiry index: {lease_expiry}")
    print(f"Lease duration: {lease_expiry - lease_start} entries")
    
    # Verify lease is valid initially
    assert state.is_lease_valid(task, state.current_log_index), "Lease should be valid"
    print("Lease is valid initially")
    
    # Simulate time passing (advance log index)
    state.current_log_index = lease_expiry - 1
    task = state.get_task(task.id)  # Refresh
    assert state.is_lease_valid(task, state.current_log_index), "Lease should still be valid"
    print(f"At index {state.current_log_index}: lease still valid")
    
    # Advance past expiry
    state.current_log_index = lease_expiry
    task = state.get_task(task.id)  # Refresh
    assert not state.is_lease_valid(task, state.current_log_index), "Lease should be expired"
    print(f"At index {state.current_log_index}: lease EXPIRED")
    
    # Check expired leases
    expired = state.check_expired_leases()
    assert len(expired) == 1, "Should have one expired task"
    assert expired[0].id == task.id, "Wrong task expired"
    
    print("PASS: Log-based lease expiration works correctly")


def test_deterministic_time():
    """Test that state machine uses log index for time, not wall clock."""
    print_test("Test: Deterministic Time (No datetime.now())")
    
    state = ProcessTableState(shard_id=0)
    
    # Verify initial state
    assert state.current_log_index == 0, "Initial log index should be 0"
    
    # Submit a task at index 0
    result = state.apply_command("submit_task", {
        "name": "time-test",
        "namespace": "default",
        "image": "alpine",
    })
    task = result.task
    
    created_at = task.created_at_index
    print(f"Task created_at_index: {created_at}")
    
    # Advance log index
    state.advance_log_index()
    state.advance_log_index()
    state.advance_log_index()
    
    print(f"After 3 advances, current_log_index: {state.current_log_index}")
    
    # Submit another task
    result = state.apply_command("submit_task", {
        "name": "time-test-2",
        "namespace": "default",
        "image": "alpine",
    })
    task2 = result.task
    
    print(f"Task 2 created_at_index: {task2.created_at_index}")
    
    # Verify time is based on log index
    assert task2.created_at_index > task.created_at_index, "Later task should have higher index"
    
    print("PASS: Time is deterministic (based on log index)")


def test_sqlite_storage():
    """Test SQLite storage backend."""
    print_test("Test: SQLite WAL Storage Backend")
    
    # Create temporary directory
    temp_dir = tempfile.mkdtemp()
    
    try:
        storage = SQLiteWALStorage(node_id=1, data_dir=temp_dir)
        
        # Test term persistence
        asyncio.run(storage.save_term(42))
        loaded_term = asyncio.run(storage.load_term())
        assert loaded_term == 42, f"Term mismatch: {loaded_term} != 42"
        print("Term persistence: OK")
        
        # Test vote persistence
        asyncio.run(storage.save_vote(5))
        loaded_vote = asyncio.run(storage.load_vote())
        assert loaded_vote == 5, f"Vote mismatch: {loaded_vote} != 5"
        print("Vote persistence: OK")
        
        # Test log entry persistence
        entry = asyncio.run(storage.get_last_log_index())
        print(f"Initial last log index: {entry}")
        
        from aethergrid.storage import LogEntry
        entry = LogEntry(term=1, index=1, command=b'{"type":"test"}')
        asyncio.run(storage.append_log_entry(entry))
        
        last_idx = asyncio.run(storage.get_last_log_index())
        assert last_idx == 1, f"Last index mismatch: {last_idx} != 1"
        print("Log entry persistence: OK")
        
        # Test snapshot
        from aethergrid.storage import Snapshot
        snapshot = Snapshot(
            last_included_index=10,
            last_included_term=1,
            data=b'{"test": "data"}',
        )
        asyncio.run(storage.save_snapshot(snapshot))
        
        loaded = asyncio.run(storage.load_snapshot())
        assert loaded is not None, "Snapshot not loaded"
        assert loaded.last_included_index == 10, "Snapshot index mismatch"
        print("Snapshot persistence: OK")
        
        # Get stats
        stats = storage.get_stats()
        print(f"Storage stats: {stats}")
        
        asyncio.run(storage.close())
        print("PASS: SQLite storage backend works")
    
    finally:
        # Cleanup
        shutil.rmtree(temp_dir)


def test_state_machine_serialization():
    """Test state machine serialization for snapshots."""
    print_test("Test: State Machine Serialization")
    
    state = ProcessTableState(shard_id=0)
    
    # Add some tasks
    for i in range(5):
        state.apply_command("submit_task", {
            "name": f"task-{i}",
            "namespace": "default",
            "image": "alpine",
        })
    
    # Add a node
    state.apply_command("register_node", {
        "node_id": {"id": "worker-1"},
        "hostname": "worker-1.local",
        "capacity": {"cpu_millis": 4000, "memory_bytes": 8589934592},
    })
    
    print(f"State has {len(state.tasks)} tasks and {len(state.nodes)} nodes")
    
    # Serialize
    snapshot_data = state.to_snapshot()
    print(f"Snapshot size: {len(snapshot_data)} bytes (compressed)")
    
    # Deserialize
    restored = ProcessTableState.from_snapshot(snapshot_data)
    
    # Verify
    assert len(restored.tasks) == len(state.tasks), "Task count mismatch"
    assert len(restored.nodes) == len(state.nodes), "Node count mismatch"
    assert restored.next_task_sequence == state.next_task_sequence, "Sequence mismatch"
    assert restored.next_fencing_token == state.next_fencing_token, "Fencing token mismatch"
    
    print("PASS: State machine serialization works")


def test_fencing_token_validator():
    """Test the FencingTokenValidator utility."""
    print_test("Test: Fencing Token Validator")
    
    validator = FencingTokenValidator()
    
    # Valid token
    is_valid, error = validator.validate(100, 100)
    assert is_valid, "Valid token should be accepted"
    print("Valid token: accepted")
    
    # Stale token (zombie worker)
    is_valid, error = validator.validate(100, 50)
    assert not is_valid, "Stale token should be rejected"
    assert "Stale" in error, f"Wrong error: {error}"
    print(f"Stale token: rejected ({error})")
    
    # Check zombie detection
    assert validator.is_zombie_worker(100, 50), "Should detect zombie"
    assert not validator.is_zombie_worker(100, 100), "Should not detect zombie"
    print("Zombie detection: OK")
    
    print("PASS: Fencing token validator works")


def run_all_tests():
    """Run all verification tests."""
    print_header("AetherGrid v2.0 - Zombie Worker Protection Verification")
    
    tests = [
        ("Fencing Token Generation", test_fencing_token_generation),
        ("Valid Fencing Token", test_valid_fencing_token),
        ("Zombie Worker Rejection", test_zombie_worker_rejected),
        ("Task Reassignment Invalidation", test_task_reassignment_invalidation),
        ("Log-Based Lease", test_log_based_lease),
        ("Deterministic Time", test_deterministic_time),
        ("SQLite Storage", test_sqlite_storage),
        ("State Machine Serialization", test_state_machine_serialization),
        ("Fencing Token Validator", test_fencing_token_validator),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            test_func()
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
    success = run_all_tests()
    sys.exit(0 if success else 1)
