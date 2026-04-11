"""AetherGrid Core - ClusterSimulator.

Simulates a network of Raft nodes with message passing and partition support.
"""

import asyncio
from typing import Dict, List, Optional, Set, Tuple

from models import AppendEntries, AppendEntriesResponse, RequestVote, RequestVoteResponse
from node import RaftNode


class ClusterSimulator:
    """Simulates a cluster of Raft nodes."""

    def __init__(self, node_ids: List[int], message_delay: float = 0.0) -> None:
        """Initialize simulator with given node IDs and optional message delay."""
        self.node_ids = node_ids
        self.nodes: Dict[int, RaftNode] = {}
        self.inboxes: Dict[int, asyncio.Queue] = {}
        self.connected: Set[int] = set(node_ids)  # All nodes connected initially
        self.tasks: List[asyncio.Task] = []
        self.message_log: List[Tuple[int, int, object]] = []  # (from, to, msg)
        self.message_delay: float = message_delay  # Delay for all messages (seconds)

    def _send_message(
        self, sender_id: int, target_id: int, message: object, delay: float = 0.0
    ) -> None:
        """Send a message from sender to target if connected, optionally with delay."""
        # Record for debugging
        self.message_log.append((sender_id, target_id, message))

        # Only deliver if both nodes are connected
        if sender_id in self.connected and target_id in self.connected:
            if target_id in self.inboxes:
                if delay > 0:
                    asyncio.create_task(
                        self._delayed_delivery(target_id, sender_id, message, delay)
                    )
                else:
                    self.inboxes[target_id].put_nowait((sender_id, message))

    async def _delayed_delivery(
        self, target_id: int, sender_id: int, message: object, delay: float
    ) -> None:
        """Deliver a message after a delay."""
        await asyncio.sleep(delay)
        if target_id in self.inboxes and sender_id in self.connected and target_id in self.connected:
            self.inboxes[target_id].put_nowait((sender_id, message))

    def create_nodes(
        self,
        election_timeout_min: float = 0.150,
        election_timeout_max: float = 0.300,
        heartbeat_interval: float = 0.050,
    ) -> None:
        """Create all RaftNode instances with optional timeout configuration."""
        for node_id in self.node_ids:
            peers = set(self.node_ids) - {node_id}
            inbox: asyncio.Queue = asyncio.Queue()
            self.inboxes[node_id] = inbox

            def make_sender(nid: int) -> callable:
                def send(target: int, msg: object) -> None:
                    self._send_message(nid, target, msg, delay=self.message_delay)
                return send

            node = RaftNode(
                node_id=node_id,
                peers=peers,
                inbox=inbox,
                send_message=make_sender(node_id),
                election_timeout_min=election_timeout_min,
                election_timeout_max=election_timeout_max,
                heartbeat_interval=heartbeat_interval,
            )
            self.nodes[node_id] = node

    def partition_node(self, node_id: int) -> None:
        """Partition a node from the rest of the cluster."""
        self.connected.discard(node_id)

    def heal_partition(self, node_id: int) -> None:
        """Heal partition for a node."""
        if node_id in self.node_ids:
            self.connected.add(node_id)

    def heal_all(self) -> None:
        """Heal all partitions."""
        self.connected = set(self.node_ids)

    def get_leader(self) -> Optional[int]:
        """Return the current leader node ID, if any."""
        for node_id, node in self.nodes.items():
            if node.state.value == "leader":
                return node_id
        return None

    def get_leaders(self) -> List[int]:
        """Return all nodes currently in leader state."""
        leaders = []
        for node_id, node in self.nodes.items():
            if node.state.value == "leader":
                leaders.append(node_id)
        return leaders

    async def run(self, duration: float = 5.0) -> None:
        """Run the simulation for a given duration (seconds)."""
        # Create nodes if not created
        if not self.nodes:
            self.create_nodes()

        # Start all node tasks
        self.tasks = [
            asyncio.create_task(node.run()) for node in self.nodes.values()
        ]

        try:
            await asyncio.sleep(duration)
        finally:
            # Cancel all tasks
            for task in self.tasks:
                task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)
            self.tasks.clear()

    def get_node_states(self) -> Dict[int, str]:
        """Return current state of all nodes."""
        return {nid: node.state.value for nid, node in self.nodes.items()}

    def get_node_terms(self) -> Dict[int, int]:
        """Return current term of all nodes."""
        return {nid: node.current_term for nid, node in self.nodes.items()}

    def register_node(self, node: "RaftNode") -> None:
        """Register a node with the simulator and update all peers."""
        nid = node.node_id
        self.nodes[nid] = node
        self.node_ids.append(nid)
        self.inboxes[nid] = node.inbox
        self.connected.add(nid)
        # Update all nodes' peers to include this node
        for existing_id, existing_node in self.nodes.items():
            if existing_id != nid:
                existing_node.peers.add(nid)
                if hasattr(existing_node, "next_index"):
                    existing_node.next_index[nid] = existing_node.last_log_index + 1
                if hasattr(existing_node, "match_index"):
                    existing_node.match_index[nid] = existing_node.last_included_index

    def client_request(self, command: dict) -> bool:
        """Submit a command to the current leader. Returns True if submitted, False otherwise."""
        leader_id = self.get_leader()
        if leader_id is None:
            return False
        leader = self.nodes.get(leader_id)
        if leader is None:
            return False
        # Submit command to leader
        leader.append_entry(command)
        return True

    async def client_read(self, key: str) -> tuple:
        """Read a key using linearizable reads with redirect handling.

        Returns: (value, error) where error is None on success or ("NotLeader", leader_id).
        """
        # Try to find a leader and read
        for _ in range(5):  # Retry up to 5 times
            leader_id = self.get_leader()
            if leader_id is None:
                await asyncio.sleep(0.05)
                continue
            leader = self.nodes.get(leader_id)
            if leader is None:
                await asyncio.sleep(0.05)
                continue

            result, error = await leader.client_read(key)
            if error is None:
                return (result, None)
            if isinstance(error, tuple) and error[0] == "NotLeader":
                # Redirect to known leader
                new_leader_id = error[1]
                if new_leader_id is not None and new_leader_id in self.nodes:
                    await asyncio.sleep(0.01)
                    continue
            # Not a redirect or no known leader
            return (None, error)

        return (None, ("NotLeader", None))

    def get_leader_node(self) -> Optional["RaftNode"]:
        """Return the current leader node, or None if no leader exists."""
        leader_id = self.get_leader()
        if leader_id is None:
            return None
        return self.nodes.get(leader_id)
