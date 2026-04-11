#!/usr/bin/env python3
"""AetherGrid Core - Final Engine Verification Script.

Validates linearizable reads, leader leases, and partition behavior.
"""

import asyncio
import sys

from simulator import ClusterSimulator


async def verify_final_engine() -> bool:
    """Run final verification of linearizable reads and leader leases."""
    print("=== AetherGrid Core: Final Engine Verification ===")
    print()

    # Test: Write, Partition, Read (lease expiry), Read from new cluster
    print("Test: Linearizable Reads with Leader Lease")
    print("-" * 50)

    sim = ClusterSimulator(node_ids=[1, 2, 3], message_delay=0.0)
    sim.create_nodes(
        election_timeout_min=0.100,
        election_timeout_max=0.200,
        heartbeat_interval=0.040,
    )

    # Run to elect a leader
    await sim.run(duration=1.0)

    leader_id = sim.get_leader()
    if leader_id is None:
        print("FAIL: No leader elected")
        return False

    print(f"Leader elected: node {leader_id}")

    # Step 1: Write {"key": "balance", "value": "100"}
    write_cmd = {"op": "set", "key": "balance", "value": "100"}
    result = sim.client_request(write_cmd)
    if not result:
        print("FAIL: client_request failed")
        return False

    print("Submitted write: balance = 100")

    # Run to allow replication
    await sim.run(duration=1.5)

    # Step 2: Partition the current leader
    sim.partition_node(leader_id)
    print(f"Partitioned leader (node {leader_id})")

    # Give some time for partition to take effect
    await asyncio.sleep(0.1)

    # Step 3: Attempt read from old leader (should fail after lease expires)
    old_leader = sim.nodes[leader_id]

    # Wait for lease to expire (500ms + some buffer)
    await asyncio.sleep(0.6)

    # Try to read from old leader
    value, error = await old_leader.client_read("balance")

    if error is None:
        print(f"FAIL: Old leader still serving reads after lease expiry (value={value})")
        return False

    if isinstance(error, tuple) and error[0] == "NotLeader":
        print("PASS: Old leader refuses to answer read after lease expiry")
    else:
        print(f"Note: Old leader returned error: {error}")

    # Step 4: Read from new cluster should return "100"
    # Heal all first, wait for new election
    sim.heal_all()

    # Run to allow new election
    await sim.run(duration=2.0)

    new_leader_id = sim.get_leader()
    if new_leader_id is None:
        print("FAIL: No new leader elected after healing")
        return False

    if new_leader_id == leader_id:
        print("Note: Same leader re-elected (expected new leader)")

    print(f"New leader elected: node {new_leader_id}")

    # Read from new cluster
    value, error = await sim.client_read("balance")

    if error is not None:
        print(f"FAIL: Read from new cluster failed: {error}")
        return False

    if value != "100":
        print(f"FAIL: Read returned {value} instead of 100")
        return False

    print(f"PASS: Read from new cluster returns '100'")

    print()
    print("=" * 50)
    print("All final engine verification tests PASSED!")
    print("=" * 50)
    return True


def main() -> int:
    """Main entry point."""
    try:
        success = asyncio.run(verify_final_engine())
        return 0 if success else 1
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
