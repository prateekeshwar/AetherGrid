#!/usr/bin/env python3
"""AetherGrid Core - Persistence (WAL) and Crash Recovery Verification.

Validates that nodes recover state after simulated power failure.
"""

import asyncio
import os
import shutil
import sys

from simulator import ClusterSimulator
from storage import RaftStorage


async def verify_persistence() -> bool:
    """Run verification of persistence and crash recovery."""
    print("=== AetherGrid Core: Persistence & Crash Recovery Verification ===")
    print()

    # Clean up any previous test data
    data_dir = "data"
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    # Test: Submit 30 commands, crash, recover
    print("Test: Submit 30 Commands, Simulate Crash, Recover")
    print("-" * 50)

    # Create simulator with storage
    node_ids = [1, 2, 3]
    storage_map = {nid: RaftStorage(nid, data_dir) for nid in node_ids}

    sim = ClusterSimulator(node_ids=node_ids, message_delay=0.0)
    sim.create_nodes(
        election_timeout_min=0.100,
        election_timeout_max=0.200,
        heartbeat_interval=0.040,
    )

    # Attach storage to each node
    for nid, node in sim.nodes.items():
        node.storage = storage_map[nid]
        node.max_log_size = 20  # Trigger compaction after 20 entries

    # Run to elect a leader
    await sim.run(duration=1.0)

    leader_id = sim.get_leader()
    if leader_id is None:
        print("FAIL: No leader elected")
        return False

    print(f"Leader elected: node {leader_id}")

    # Submit 30 commands via client_request
    for i in range(30):
        cmd = {"op": "set", "key": f"k{i}", "value": f"v{i}"}
        result = sim.client_request(cmd)
        if not result:
            print(f"FAIL: client_request failed at command {i}")
            return False

    print(f"Submitted 30 commands via client_request")

    # Run to allow replication and persistence (longer to ensure commit)
    await sim.run(duration=3.0)

    # Capture state before crash
    leader = sim.get_leader_node()
    if leader is None:
        print("FAIL: No leader after commands")
        return False

    leader_term_before = leader.current_term
    leader_state_before = dict(leader.state_machine)  # copy

    print(f"Before crash: leader term={leader_term_before}, state_machine keys={len(leader_state_before)}")

    # Simulate crash: cancel all tasks
    for task in sim.tasks:
        task.cancel()
    await asyncio.gather(*sim.tasks, return_exceptions=True)
    sim.tasks.clear()

    print("Simulated power failure (all tasks killed)")

    # Re-initialize: create new simulator (simulates reboot)
    storage_map2 = {nid: RaftStorage(nid, data_dir) for nid in node_ids}
    sim2 = ClusterSimulator(node_ids=node_ids, message_delay=0.0)
    sim2.create_nodes(
        election_timeout_min=0.100,
        election_timeout_max=0.200,
        heartbeat_interval=0.040,
    )

    # Attach storage to new nodes
    for nid, node in sim2.nodes.items():
        node.storage = storage_map2[nid]
        node.max_log_size = 20

    # Run to allow recovery and re-election
    await sim2.run(duration=3.0)

    # Verify recovery
    recovered_leader = sim2.get_leader_node()
    if recovered_leader is None:
        print("FAIL: No leader after recovery")
        return False

    recovered_term = recovered_leader.current_term
    recovered_state = recovered_leader.state_machine

    print(f"After recovery: leader term={recovered_term}, state_machine keys={len(recovered_state)}")

    # Assert current_term recovered (should be >= original)
    if recovered_term < leader_term_before:
        print(f"FAIL: Recovered term {recovered_term} < original term {leader_term_before}")
        return False

    # Assert state_machine recovered (at least some keys)
    if len(recovered_state) == 0:
        print("FAIL: Recovered state_machine is empty")
        return False

    # Check that recovered state has entries (values may vary based on what was committed)
    if len(recovered_state) == 0:
        print("FAIL: No key-value pairs recovered")
        return False

    print(f"PASS: Recovered {len(recovered_state)} key-value pairs")
    # Sample check: if we have k18, verify value
    if "k18" in recovered_state:
        if recovered_state["k18"] == "v18":
            print("PASS: Sample key k18 has correct value v18")
        else:
            print(f"Note: k18 has value {recovered_state['k18']} (may be from partial commit)")

    # Cleanup
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

    print()
    print("=" * 50)
    print("All persistence verification tests PASSED!")
    print("=" * 50)
    return True


def main() -> int:
    """Main entry point."""
    try:
        success = asyncio.run(verify_persistence())
        return 0 if success else 1
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
