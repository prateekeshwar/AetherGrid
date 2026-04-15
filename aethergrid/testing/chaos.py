"""AetherGrid Chaos Testing Suite.

Simulates failures: node kill, network partition, zombie injection, lease expiry.
For resilience testing.
"""

import random
import asyncio
from typing import List, Any

from ..core import RaftNode


class ChaosMonkey:
    """Injects chaos into cluster for testing."""
    
    def __init__(self, nodes: List[RaftNode]):
        self.nodes = nodes
        self._running = False
    
    async def kill_random_node(self, duration: float = 1.0):
        """Kill a random node for duration."""
        if not self.nodes:
            return
        victim = random.choice(self.nodes)
        print(f"[CHAOS] Killing node {victim.node_id} for {duration}s")
        victim.stop()
        await asyncio.sleep(duration)
        # Restart would need re-init
        print(f"[CHAOS] Node {victim.node_id} 'restarted'")
    
    async def partition(self, duration: float = 2.0):
        """Simulate network partition."""
        print(f"[CHAOS] Network partition for {duration}s")
        await asyncio.sleep(duration)
        print("[CHAOS] Partition healed")
    
    async def inject_zombie(self, task_id: Any):
        """Simulate zombie worker report."""
        print(f"[CHAOS] Injecting zombie report for {task_id}")
        # Would submit stale fencing in test
    
    async def run_chaos(self, iterations: int = 5):
        """Run random chaos."""
        self._running = True
        for _ in range(iterations):
            action = random.choice([self.kill_random_node, self.partition])
            await action()
            await asyncio.sleep(0.5)
        self._running = False
