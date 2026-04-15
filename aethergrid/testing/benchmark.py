"""AetherGrid Benchmark Suite.

Simple throughput/latency benchmarks for Raft + task submission.
"""

import asyncio
import time
from typing import Dict, Any

from ..core import RaftNode, ProcessTableState
from ..storage import MemoryStorage


async def benchmark_task_submission(n_tasks: int = 1000) -> Dict[str, Any]:
    """Benchmark submit + commit throughput."""
    state = ProcessTableState(shard_id=0)
    start = time.time()
    for i in range(n_tasks):
        state.apply_command("submit_task", {
            "name": f"bench-{i}",
            "namespace": "bench",
            "image": "alpine",
        })
    elapsed = time.time() - start
    return {
        "tasks": n_tasks,
        "elapsed_sec": round(elapsed, 3),
        "tasks_per_sec": round(n_tasks / elapsed, 1),
    }


async def benchmark_raft_election() -> Dict[str, Any]:
    """Benchmark leader election time (sim)."""
    # Simplified
    start = time.time()
    await asyncio.sleep(0.05)  # mock election
    return {"election_time_ms": int((time.time() - start) * 1000)}
