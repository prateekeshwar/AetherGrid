"""AetherGrid Core - RaftNode Implementation.

Core Raft node with leader election logic.
"""

import asyncio
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Set

from models import (
    AppendEntries,
    AppendEntriesResponse,
    InstallSnapshotRequest,
    LogEntry,
    RaftState,
    RequestVote,
    RequestVoteResponse,
)

# Optional storage import
try:
    from storage import RaftStorage
except ImportError:
    RaftStorage = None  # type: ignore


@dataclass
class RaftNode:
    """A single Raft node in the cluster."""

    node_id: int
    peers: Set[int]
    inbox: asyncio.Queue
    send_message: Callable[[int, object], None]
    storage: Optional["RaftStorage"] = None  # Optional persistent storage

    # Persistent state
    current_term: int = 0
    voted_for: Optional[int] = None

    # Automatic compaction threshold
    max_log_size: int = 50

    # Volatile state
    state: RaftState = RaftState.FOLLOWER
    commit_index: int = 0
    last_applied: int = 0

    # Snapshot state (log compaction)
    last_included_index: int = 0
    last_included_term: int = 0
    # Simple state machine for applied commands (key-value store)
    state_machine: dict = field(default_factory=dict)

    # Log storage (each entry is a dict with 'term' and 'command')
    # log[0] corresponds to index last_included_index + 1 (no dummy entries)
    log: list = field(default_factory=list)

    # Leader state
    next_index: dict = field(default_factory=dict)
    match_index: dict = field(default_factory=dict)

    # Election state
    votes_received: Set[int] = field(default_factory=set)
    election_timeout: float = 0.0
    last_heartbeat: float = 0.0

    # Leader Lease state
    leader_lease_expiry: float = 0.0  # Timestamp when lease expires
    last_heartbeat_acks: Set[int] = field(default_factory=set)  # Peers that acked last heartbeat

    # Timing constants (in seconds) - can be overridden per-instance
    election_timeout_min: float = 0.150
    election_timeout_max: float = 0.300
    heartbeat_interval: float = 0.050
    LEASE_DURATION: float = 0.500  # 500ms leader lease for reads

    def reset_election_timeout(self) -> None:
        """Reset election timeout with random value."""
        self.election_timeout = random.uniform(
            self.election_timeout_min, self.election_timeout_max
        )
        self.last_heartbeat = asyncio.get_event_loop().time()

    @property
    def last_log_index(self) -> int:
        """Return the index of the last log entry (last_included_index if log empty)."""
        if not self.log:
            return self.last_included_index
        return self.last_included_index + len(self.log)

    @property
    def last_log_term(self) -> int:
        """Return the term of the last log entry (last_included_term if log empty)."""
        if not self.log:
            return self.last_included_term
        return self.log[-1]["term"]

    def become_follower(self, term: int) -> None:
        """Transition to follower state with given term."""
        self.state = RaftState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.votes_received.clear()
        self.reset_election_timeout()
        # Persist term and vote
        if self.storage is not None:
            asyncio.create_task(self.storage.save_term(term))
            asyncio.create_task(self.storage.save_vote(None))

    def become_candidate(self) -> None:
        """Transition to candidate state and request votes."""
        self.state = RaftState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.reset_election_timeout()
        # Persist term and vote
        if self.storage is not None:
            asyncio.create_task(self.storage.save_term(self.current_term))
            asyncio.create_task(self.storage.save_vote(self.node_id))

        # Request votes from all peers with actual log info
        for peer_id in self.peers:
            request = RequestVote(
                term=self.current_term,
                candidate_id=self.node_id,
                last_log_index=self.last_log_index,
                last_log_term=self.last_log_term,
            )
            self.send_message(peer_id, request)

    def become_leader(self) -> None:
        """Transition to leader state and start heartbeats (snapshot-aware)."""
        self.state = RaftState.LEADER
        self.votes_received.clear()

        # Initialize leader state - next_index starts at last_log_index + 1
        for peer_id in self.peers:
            self.next_index[peer_id] = self.last_log_index + 1
            self.match_index[peer_id] = self.last_included_index

        # Send immediate heartbeat to all peers
        self.send_heartbeats()

    def send_heartbeats(self) -> None:
        """Send AppendEntries (heartbeat) to all peers, including any pending log entries."""
        for peer_id in self.peers:
            if peer_id in self.next_index:
                self._send_entries_to_peer(peer_id)
            else:
                # Fallback for initial heartbeat
                heartbeat = AppendEntries(
                    term=self.current_term,
                    leader_id=self.node_id,
                    prev_log_index=0,
                    prev_log_term=0,
                    entries=[],
                    leader_commit=self.commit_index,
                )
                self.send_message(peer_id, heartbeat)

    def handle_request_vote(self, msg: RequestVote) -> None:
        """Handle incoming RequestVote RPC with log up-to-date check."""
        # Step-down if higher term
        if msg.term > self.current_term:
            self.become_follower(msg.term)

        vote_granted = False
        if (
            msg.term == self.current_term
            and (self.voted_for is None or self.voted_for == msg.candidate_id)
        ):
            # Check if candidate's log is at least as up-to-date
            candidate_up_to_date = (
                msg.last_log_term > self.last_log_term
                or (
                    msg.last_log_term == self.last_log_term
                    and msg.last_log_index >= self.last_log_index
                )
            )
            if candidate_up_to_date:
                self.voted_for = msg.candidate_id
                vote_granted = True
                self.reset_election_timeout()
                # Persist vote
                if self.storage is not None:
                    asyncio.create_task(self.storage.save_vote(msg.candidate_id))

        response = RequestVoteResponse(term=self.current_term, vote_granted=vote_granted)
        self.send_message(msg.candidate_id, response)

    def handle_append_entries(self, msg: AppendEntries) -> None:
        """Handle incoming AppendEntries with log replication."""
        # Step-down if higher term
        if msg.term > self.current_term:
            self.become_follower(msg.term)

        success = False
        match_index = 0
        conflict_index = None

        if msg.term == self.current_term:
            if self.state != RaftState.FOLLOWER:
                self.state = RaftState.FOLLOWER
            self.reset_election_timeout()

            # Consistency check (handles snapshot indexing)
            prev_term_ok = False
            if msg.prev_log_index == 0:
                prev_term_ok = (msg.prev_log_term == 0)
            elif msg.prev_log_index <= self.last_included_index:
                # Entry is in snapshot - match against last_included_term
                prev_term_ok = (msg.prev_log_term == self.last_included_term)
            else:
                # Entry should be in log
                pos = self._log_index_to_pos(msg.prev_log_index)
                if pos >= 0:
                    prev_term_ok = (self.log[pos]["term"] == msg.prev_log_term)

            if prev_term_ok:
                success = True
                # Truncate conflicting entries AFTER prev_log_index (not at it)
                if msg.prev_log_index > self.last_included_index:
                    pos = self._log_index_to_pos(msg.prev_log_index)
                    if pos >= 0:
                        # Keep entries up to and including prev_log_index, truncate after
                        self.log = self.log[:pos + 1]
                elif msg.prev_log_index == self.last_included_index:
                    # Truncate all log entries (they conflict after snapshot point)
                    self.log = []
                # Append new entries
                for entry in msg.entries:
                    self.log.append(entry)
                match_index = self.last_included_index + len(self.log)
                # Update commit_index and apply committed entries
                if msg.leader_commit > self.commit_index:
                    self.commit_index = min(msg.leader_commit, match_index)
                    self._apply_committed_entries()
                    asyncio.create_task(self._maybe_compact())
            else:
                # Inconsistency detected - find conflict index (snapshot-aware)
                success = False
                if msg.prev_log_index > self.last_log_index:
                    conflict_index = self.last_log_index + 1
                else:
                    # Find first conflicting term
                    for i in range(msg.prev_log_index, 0, -1):
                        term_at_i = self._get_log_term(i)
                        if term_at_i != msg.prev_log_term:
                            conflict_index = i
                            break
                    if conflict_index is None:
                        conflict_index = msg.prev_log_index

        response = AppendEntriesResponse(
            term=self.current_term,
            success=success,
            match_index=match_index if success else None,
            conflict_index=conflict_index,
        )
        self.send_message(msg.leader_id, response)

    def handle_append_entries_response(self, msg: AppendEntriesResponse) -> None:
        """Handle incoming AppendEntriesResponse with next_index retry logic."""
        if msg.term > self.current_term:
            self.become_follower(msg.term)
            return

        if self.state != RaftState.LEADER:
            return

        # Find which peer sent this (we need to track via process_message's sender_id)
        # For now, this is handled in process_message with sender_id
        pass  # Actual handling is in process_message with sender_id

    def handle_append_entries_response_with_sender(
        self, sender_id: int, msg: AppendEntriesResponse
    ) -> None:
        """Handle AppendEntriesResponse knowing the sender (snapshot-aware)."""
        if msg.term > self.current_term:
            self.become_follower(msg.term)
            return

        if self.state != RaftState.LEADER:
            return

        if msg.success:
            # Track heartbeat ack for lease
            self.last_heartbeat_acks.add(sender_id)

            # Extend lease if we have majority
            majority = (len(self.peers) + 1) // 2 + 1
            if len(self.last_heartbeat_acks) >= majority:
                self.leader_lease_expiry = asyncio.get_event_loop().time() + self.LEASE_DURATION

            # Update match_index and next_index for this peer
            if msg.match_index is not None:
                self.match_index[sender_id] = msg.match_index
                self.next_index[sender_id] = msg.match_index + 1
            # Check if we can advance commit_index
            self._update_commit_index()
        else:
            # Decrement next_index and retry (consistency fix)
            if sender_id in self.next_index:
                if msg.conflict_index is not None:
                    self.next_index[sender_id] = max(self.last_included_index + 1, msg.conflict_index)
                else:
                    self.next_index[sender_id] = max(self.last_included_index + 1, self.next_index[sender_id] - 1)
                # Retry sending entries to this peer
                self._send_entries_to_peer(sender_id)

    def _update_commit_index(self) -> None:
        """Update commit_index based on majority match (snapshot-aware)."""
        if self.last_log_index <= self.commit_index:
            return
        # Find highest N where majority of match_index >= N and term of N == current_term
        for n in range(self.last_log_index, self.commit_index, -1):
            term_n = self._get_log_term(n)
            if term_n != self.current_term:
                continue
            count = 1  # Count self
            for peer_id in self.peers:
                if self.match_index.get(peer_id, 0) >= n:
                    count += 1
            majority = (len(self.peers) + 1) // 2 + 1
            if count >= majority:
                self.commit_index = n
                self._apply_committed_entries()
                asyncio.create_task(self._maybe_compact())
                break

    def _send_entries_to_peer(self, peer_id: int) -> None:
        """Send AppendEntries or InstallSnapshot to a specific peer (snapshot-aware)."""
        if peer_id not in self.next_index:
            return
        next_idx = self.next_index[peer_id]

        # If peer is behind our snapshot, send InstallSnapshot instead
        if next_idx <= self.last_included_index:
            snapshot_data = str(self.state_machine).encode("utf-8")
            snapshot_msg = InstallSnapshotRequest(
                term=self.current_term,
                leader_id=self.node_id,
                last_included_index=self.last_included_index,
                last_included_term=self.last_included_term,
                data=snapshot_data,
            )
            self.send_message(peer_id, snapshot_msg)
            return

        prev_log_index = next_idx - 1

        # Handle snapshot boundary for prev_log_term
        if prev_log_index == 0:
            prev_log_term = 0
        elif prev_log_index <= self.last_included_index:
            prev_log_term = self.last_included_term
        else:
            pos = self._log_index_to_pos(prev_log_index)
            prev_log_term = self.log[pos]["term"] if pos >= 0 else 0

        # Get entries from next_idx onwards
        start_pos = self._log_index_to_pos(next_idx)
        if start_pos >= 0:
            entries = [
                {"term": e["term"], "command": e["command"]}
                for e in self.log[start_pos:]
            ]
        else:
            entries = []

        msg = AppendEntries(
            term=self.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.commit_index,
        )
        self.send_message(peer_id, msg)

    def append_entry(self, command: object) -> None:
        """Append a new command to the log (for leader or testing)."""
        entry = {"term": self.current_term, "command": command}
        self.log.append(entry)
        # Persist log entry
        if self.storage is not None:
            asyncio.create_task(self.storage.save_log_entry(self.last_log_index, entry))
            # Also persist state_machine (optimistic - leader applies its own entries)
            if self.state == RaftState.LEADER:
                self._apply_command_to_state_machine(command)

    async def client_read(self, key: str) -> tuple:
        """Linearizable read using ReadIndex protocol.

        Returns: (value, error) where error is None on success or ("NotLeader", leader_id).
        """
        # Check if this node is leader
        if self.state != RaftState.LEADER:
            # Find leader if we know it
            leader_id = None
            for peer_id in self.peers:
                # We don't track leader_id, so return None
                pass
            return (None, ("NotLeader", None))

        # Check if we have a valid leader lease (optimization)
        now = asyncio.get_event_loop().time()
        if now < self.leader_lease_expiry:
            # Lease valid - can serve read locally
            value = self.state_machine.get(key)
            return (value, None)

        # No valid lease - must use ReadIndex protocol
        # Step 1: Record current commit_index as read_index
        read_index = self.commit_index

        # Step 2: Confirm leadership via round of heartbeats to majority
        majority = (len(self.peers) + 1) // 2 + 1
        self.last_heartbeat_acks = {self.node_id}  # Count self

        # Send heartbeats to all peers
        for peer_id in self.peers:
            heartbeat = AppendEntries(
                term=self.current_term,
                leader_id=self.node_id,
                prev_log_index=0,
                prev_log_term=0,
                entries=[],
                leader_commit=self.commit_index,
            )
            self.send_message(peer_id, heartbeat)

        # Wait for majority acks (with timeout)
        ack_event = asyncio.Event()
        deadline = now + self.heartbeat_interval * 2  # Wait up to 2 heartbeat intervals

        while len(self.last_heartbeat_acks) < majority and now < deadline:
            await asyncio.sleep(0.001)
            now = asyncio.get_event_loop().time()

        if len(self.last_heartbeat_acks) < majority:
            # Not majority - no longer leader
            return (None, ("NotLeader", None))

        # Step 3: Wait until last_applied >= read_index
        if self.last_applied < read_index:
            # Wait for last_applied to catch up
            while self.last_applied < read_index:
                await asyncio.sleep(0.001)
                if self.state != RaftState.LEADER:
                    return (None, ("NotLeader", None))

        # Step 4: Return value from state_machine
        value = self.state_machine.get(key)
        return (value, None)

    def _apply_command_to_state_machine(self, cmd: Any) -> None:
        """Apply a single command to state_machine (incremental, O(1))."""
        if isinstance(cmd, dict):
            if "key" in cmd and "value" in cmd:
                self.state_machine[cmd["key"]] = cmd["value"]
            elif cmd.get("op") == "set" and "key" in cmd and "value" in cmd:
                self.state_machine[cmd["key"]] = cmd["value"]
        # Persist state_machine incrementally if storage available
        if self.storage is not None:
            # Always persist state_machine (even if last_included_index=0)
            asyncio.create_task(self.storage._write_json_async(
                self.storage.snapshot_file,
                {
                    "last_included_index": self.last_included_index,
                    "last_included_term": self.last_included_term,
                    "state_machine": self.state_machine,
                },
            ))

    def _apply_committed_entries(self) -> None:
        """Apply all committed but unapplied entries to state_machine (incremental)."""
        while self.last_applied < self.commit_index:
            # Apply entry at last_applied + 1
            next_idx = self.last_applied + 1
            entry = self._get_log_entry(next_idx)
            if entry is not None:
                cmd = entry.get("command")
                self._apply_command_to_state_machine(cmd)
            self.last_applied = next_idx

    def take_snapshot(self, last_included_index: int) -> None:
        """Take a snapshot: discard entries up to last_included_index (state_machine already updated incrementally)."""
        if last_included_index <= self.last_included_index:
            return  # Nothing to snapshot

        # State machine is already up-to-date (updated incrementally on commit)
        # Just update snapshot metadata and discard log entries
        snapshot_end = last_included_index - self.last_included_index
        if snapshot_end > 0 and snapshot_end <= len(self.log):
            # Update last_included_term from the last discarded entry
            self.last_included_term = self.log[snapshot_end - 1]["term"]
            self.last_included_index = last_included_index
            # Discard snapshot entries from log
            self.log = self.log[snapshot_end:]
        elif snapshot_end == 0:
            pass
        else:
            # Snapshot beyond current log
            if self.log:
                self.last_included_term = self.log[-1]["term"]
                self.last_included_index = last_included_index
                self.log = []

        # Ensure last_applied is at least snapshot point
        if last_included_index > self.last_applied:
            self.last_applied = last_included_index
        if last_included_index > self.commit_index:
            self.commit_index = last_included_index

    async def _maybe_compact(self) -> None:
        """Check if log exceeds max_log_size and trigger automatic compaction."""
        if len(self.log) > self.max_log_size:
            # Snapshot at commit_index (or last_applied)
            snapshot_idx = max(self.commit_index, self.last_applied)
            if snapshot_idx > self.last_included_index:
                self.take_snapshot(snapshot_idx)
                # Persist snapshot to disk if storage available
                if self.storage is not None:
                    await self.storage.save_snapshot(
                        self.last_included_index,
                        self.last_included_term,
                        self.state_machine,
                    )

    def _log_index_to_pos(self, idx: int) -> int:
        """Convert real log index to position in self.log. Returns -1 if not in log."""
        if idx <= self.last_included_index:
            return -1
        pos = idx - self.last_included_index - 1
        if pos < 0 or pos >= len(self.log):
            return -1
        return pos

    def _get_log_term(self, idx: int) -> int:
        """Get term for a log index (handles snapshot)."""
        if idx == 0:
            return 0
        if idx <= self.last_included_index:
            # In snapshot - use last_included_term (approximation for consistency)
            return self.last_included_term
        pos = idx - self.last_included_index - 1
        if 0 <= pos < len(self.log):
            return self.log[pos]["term"]
        return 0

    def _get_log_entry(self, idx: int) -> Optional[dict]:
        """Get log entry at real index (handles snapshot)."""
        pos = self._log_index_to_pos(idx)
        if pos < 0:
            return None
        return self.log[pos]

    def handle_install_snapshot(self, msg: InstallSnapshotRequest) -> None:
        """Handle InstallSnapshot RPC from leader."""
        # Step-down if higher term
        if msg.term > self.current_term:
            self.become_follower(msg.term)

        if msg.term < self.current_term:
            return  # Ignore stale snapshot

        # Install the snapshot: replace state machine with snapshot data
        try:
            # msg.data contains serialized state_machine (from leader)
            import ast
            snapshot_state = ast.literal_eval(msg.data.decode("utf-8"))
            if isinstance(snapshot_state, dict):
                self.state_machine = snapshot_state
        except Exception:
            pass  # If deserialization fails, just update metadata

        # Update snapshot metadata
        self.last_included_index = msg.last_included_index
        self.last_included_term = msg.last_included_term

        # Clear log entries up to snapshot point (they're in the snapshot now)
        self.log = []

        # Update commit_index and last_applied
        if msg.last_included_index > self.commit_index:
            self.commit_index = msg.last_included_index
        if msg.last_included_index > self.last_applied:
            self.last_applied = msg.last_included_index
        # State machine already replaced from snapshot data
        # Trigger compaction check
        asyncio.create_task(self._maybe_compact())

        # Reset election timeout
        self.reset_election_timeout()

    async def _recover_from_storage(self) -> None:
        """Recover node state from persistent storage (crash recovery)."""
        if self.storage is None:
            return
        try:
            data = await self.storage.load_all()
            # Restore term
            if data.get("term", 0) > 0:
                self.current_term = data["term"]
            # Restore vote
            self.voted_for = data.get("voted_for")
            # Restore snapshot
            snap = data.get("snapshot", {})
            self.last_included_index = snap.get("last_included_index", 0)
            self.last_included_term = snap.get("last_included_term", 0)
            self.state_machine = snap.get("state_machine", {})
            # Restore log entries
            self.log = data.get("log", [])
            # Set last_applied and commit_index based on snapshot
            if self.last_included_index > 0:
                self.last_applied = self.last_included_index
                self.commit_index = self.last_included_index
            # If no snapshot but we have log entries, replay them to rebuild state_machine
            elif self.log:
                # Replay committed log entries (up to commit_index)
                for entry in self.log:
                    if entry is not None:
                        cmd = entry.get("command")
                        self._apply_command_to_state_machine(cmd)
                self.last_applied = min(self.commit_index, len(self.log))
        except Exception:
            # If recovery fails, start fresh
            pass

    async def run(self) -> None:
        """Main event loop for the Raft node with crash recovery."""
        # Crash recovery: load persisted state if storage available
        if self.storage is not None:
            await self._recover_from_storage()

        self.reset_election_timeout()

        while True:
            # Calculate time until next timer fires
            now = asyncio.get_event_loop().time()
            elapsed = now - self.last_heartbeat

            # Check election timeout immediately
            if self.state != RaftState.LEADER and elapsed >= self.election_timeout:
                self.become_candidate()
                continue

            # Leader sends periodic heartbeats
            if self.state == RaftState.LEADER:
                if elapsed >= self.heartbeat_interval:
                    self.send_heartbeats()
                    self.last_heartbeat = asyncio.get_event_loop().time()
                    continue

            # Calculate wait time for next timer
            if self.state == RaftState.LEADER:
                wait_time = max(0.001, self.heartbeat_interval - elapsed)
            else:
                wait_time = max(0.001, self.election_timeout - elapsed)

            try:
                # Wait for message or timeout - CPU efficient
                sender_id, message = await asyncio.wait_for(
                    self.inbox.get(), timeout=wait_time
                )
                await self.process_message(sender_id, message)
            except asyncio.TimeoutError:
                pass  # Timer fired, loop to check timers

    async def process_message(self, sender_id: int, message: object) -> None:
        """Process a single incoming message."""
        if isinstance(message, RequestVote):
            self.handle_request_vote(message)
        elif isinstance(message, RequestVoteResponse):
            # Track vote from sender
            if (
                self.state == RaftState.CANDIDATE
                and message.term == self.current_term
                and message.vote_granted
            ):
                self.votes_received.add(sender_id)
                # Check if majority reached
                majority = (len(self.peers) + 1) // 2 + 1
                if len(self.votes_received) >= majority:
                    self.become_leader()
        elif isinstance(message, AppendEntries):
            self.handle_append_entries(message)
        elif isinstance(message, AppendEntriesResponse):
            self.handle_append_entries_response_with_sender(sender_id, message)
        elif isinstance(message, InstallSnapshotRequest):
            self.handle_install_snapshot(message)
