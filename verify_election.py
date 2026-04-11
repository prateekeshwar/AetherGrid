#!/usr/bin/env python3
"""AetherGrid Core - Leader Election Verification Script.

Validates Raft leader election with network partition recovery.
"""

import asyncio
import sys

from simulator import ClusterSimulator


async def verify_leader_election() -> bool:
    """Run verification of leader election logic."""
    print("=== AetherGrid Core: Leader Election Verification ===")
    print()

    # Test 1: Initial leader election with 3 nodes
    print("Test 1: Initial Leader Election (3 nodes)")
    print("-" * 40)

    sim = ClusterSimulator(node_ids=[1, 2, 3])
    sim.create_nodes(
        election_timeout_min=0.050,
        election_timeout_max=0.100,
        heartbeat_interval=0.025,
    )

    # Run simulation for 2 seconds to allow election
    await sim.run(duration=2.0)

    leader = sim.get_leader()
    states = sim.get_node_states()
    terms = sim.get_node_terms()

    print(f"Node states: {states}")
    print(f"Node terms:  {terms}")
    print(f"Leader: {leader}")

    if leader is None:
        print("FAIL: No leader elected in initial election")
        return False

    print("PASS: Leader elected successfully")
    print()

    # Test 2: Partition leader and elect new leader
    print("Test 2: Leader Partition and New Election")
    print("-" * 40)

    # Create fresh simulator for partition test with low timeouts (50ms)
    sim2 = ClusterSimulator(node_ids=[1, 2, 3])
    sim2.create_nodes(
        election_timeout_min=0.050,
        election_timeout_max=0.100,
        heartbeat_interval=0.025,
    )

    # Run to elect initial leader
    await sim2.run(duration=2.0)

    leader2 = sim2.get_leader()
    if leader2 is None:
        print("FAIL: No initial leader in partition test")
        return False

    print(f"Initial leader: node {leader2}")

    # Partition the leader
    sim2.partition_node(leader2)

    # Run again - remaining 2 nodes should elect a new leader naturally
    # (low timeouts set in create_nodes allow natural election)
    await sim2.run(duration=2.0)

    remaining_states = sim2.get_node_states()
    remaining_terms = sim2.get_node_terms()

    print(f"After partition:")
    print(f"  Node states: {remaining_states}")
    print(f"  Node terms:  {remaining_terms}")

    # Check that among connected nodes, there is a new leader (not the partitioned one)
    connected_leaders = [
        nid for nid in sim2.connected if sim2.nodes[nid].state.value == "leader"
    ]
    print(f"  Connected leaders: {connected_leaders}")

    if not connected_leaders:
        print("FAIL: No new leader elected among remaining nodes")
        return False

    if leader2 in connected_leaders:
        print("FAIL: Partitioned node still thinks it's leader among connected nodes")
        return False

    # Also check partitioned node is not leader (it should have stepped down if it heard higher term)
    partitioned_node = sim2.nodes[leader2]
    if partitioned_node.state.value == "leader":
        # Note: partitioned node may still think it's leader since it can't hear from others
        # This is acceptable in Raft during partition (split-brain scenario)
        print(f"  (Note: Partitioned node {leader2} still has leader state - expected in split-brain)")

    print("PASS: New leader elected among remaining nodes")
    print()

    print("=" * 50)
    print("All verification tests PASSED!")
    print("=" * 50)
    return True


def main() -> int:
    """Main entry point."""
    try:
        success = asyncio.run(verify_leader_election())
        return 0 if success else 1
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
