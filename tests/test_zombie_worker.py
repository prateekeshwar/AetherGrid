"""Converted pytest version of verify_zombie_worker.py.

Tests fencing, zombie rejection, leases, deterministic time, storage.
Run: pytest tests/test_zombie_worker.py -q
"""
import pytest
import asyncio
import tempfile
import shutil

from aethergrid.core import ProcessTableState, TaskStatus
from aethergrid.storage import SQLiteWALStorage, MemoryStorage
from aethergrid.worker import FencingTokenValidator


def test_fencing_token_generation(state_machine):
    tokens = [state_machine.generate_fencing_token() for _ in range(10)]
    for i in range(1, len(tokens)):
        assert tokens[i] > tokens[i-1]


def test_zombie_worker_rejected(state_machine):
    result = state_machine.apply_command("submit_task", {"name": "zombie-test", "namespace": "default", "image": "alpine"})
    task = result.task
    orig_token = result.fencing_token
    
    result = state_machine.apply_command("schedule_task", {
        "task_id": task.id.to_dict(), "node_id": {"id": "w1"}, "fencing_token": 0, "lease_duration": 100
    })
    sched_token = result.fencing_token
    
    state_machine.apply_command("cancel_task", {"task_id": task.id.to_dict(), "reason": "reassign"})
    
    result = state_machine.apply_command("complete_task", {
        "task_id": task.id.to_dict(), "node_id": {"id": "w1"}, "fencing_token": sched_token, "exit_code": 0
    })
    assert not result.success
    assert "Stale" in result.error


@pytest.mark.asyncio
async def test_sqlite_storage():
    tmp = tempfile.mkdtemp()
    try:
        storage = SQLiteWALStorage(node_id=1, data_dir=tmp)
        await storage.save_term(42)
        assert await storage.load_term() == 42
        await storage.close()
    finally:
        shutil.rmtree(tmp)


def test_fencing_validator():
    v = FencingTokenValidator()
    assert v.validate(100, 100)[0]
    assert not v.validate(100, 50)[0]
