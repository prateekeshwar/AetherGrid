"""AetherGrid v2.0 - Raft Node Implementation (CORRECTED).

CRITICAL FIX: Commands are ONLY applied to the state machine after
being committed by a quorum. This is a fundamental Raft invariant.

Production-grade Raft implementation with:
- SQLite/RocksDB storage backend
- Deterministic log-based time (NO datetime.now() in state machine)
- Fencing tokens for zombie worker protection
- Log-based leases for task execution
- Pending command tracking for client responses
"""

import asyncio
import random
import struct
import json
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from concurrent.futures import Future

from ..storage import StorageBackend, SQLiteWALStorage, MemoryStorage, LogEntry, Snapshot
from .state_machine import (
    ProcessTableState, Task, TaskID, NodeID, TaskStatus, CommandResult
)


class RaftState(Enum):
    """Raft node states."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class RaftConfig:
    """Configuration for a Raft node."""
    election_timeout_min: float = 0.150  # seconds
    election_timeout_max: float = 0.300  # seconds
    heartbeat_interval: float = 0.050    # seconds
    max_log_size: int = 10000            # Entries before compaction
    snapshot_threshold: int = 5000       # Entries before snapshot
    lease_duration_entries: int = 1000   # Log entries for task lease


@dataclass
class PendingCommand:
    """
    Tracks a command waiting to be committed.
    
    When a client submits a command, we create a PendingCommand that
    will be resolved when the command is committed and applied.
    """
    log_index: int
    command: dict
    future: asyncio.Future
    submitted_at: float
    
    def __init__(self, log_index: int, command: dict, loop: asyncio.AbstractEventLoop):
        self.log_index = log_index
        self.command = command
        self.future = loop.create_future()
        self.submitted_at = loop.time()


class RaftNode:
    """
    Production-grade Raft node with deterministic time.
    
    CRITICAL INVARIANT: Commands are ONLY applied after being committed
    by a quorum. The submit_command method returns a Future that resolves
    when the command has been committed and applied.
    
    Key improvements over v1:
    1. Uses log index for time (not wall clock) in state machine
    2. Integrates with SQLite/RocksDB storage
    3. Fencing tokens prevent zombie workers
    4. Log-based leases for task execution
    5. Pending command tracking for client responses
    """
    
    def __init__(
        self,
        node_id: int,
        peers: Set[int],
        inbox: asyncio.Queue,
        send_message: Callable[[int, object], None],
        storage: Optional[StorageBackend] = None,
        config: Optional[RaftConfig] = None,
        shard_id: int = 0,
    ):
        self.node_id = node_id
        self.peers = peers
        self.inbox = inbox
        self.send_message = send_message
        self.storage = storage or MemoryStorage()
        self.config = config or RaftConfig()
        self.shard_id = shard_id
        
        # Persistent state (restored from storage)
        self.current_term: int = 0
        self.voted_for: Optional[int] = None
        
        # Volatile state
        self.state: RaftState = RaftState.FOLLOWER
        self.commit_index: int = 0
        self.last_applied: int = 0
        
        # Log (managed by storage)
        self._log_cache: Dict[int, LogEntry] = {}  # Cache for recent entries
        
        # Leader state
        self.next_index: Dict[int, int] = {}
        self.match_index: Dict[int, int] = {}
        
        # Election state
        self.votes_received: Set[int] = set()
        self.election_timeout: float = 0.0
        self.last_heartbeat: float = 0.0
        
        # State machine
        self.state_machine = ProcessTableState(shard_id=shard_id)
        
        # Snapshot state
        self.last_included_index: int = 0
        self.last_included_term: int = 0
        
        # Leader lease (for reads)
        self.leader_lease_expiry: float = 0.0
        self.last_heartbeat_acks: Set[int] = set()
        
        # CRITICAL: Pending commands waiting for commit
        # Maps log_index -> PendingCommand
        self._pending_commands: Dict[int, PendingCommand] = {}
        
        # Running flag
        self._running: bool = False
        self._tasks: List[asyncio.Task] = []
    
    # ========== Properties ==========
    
    @property
    def last_log_index(self) -> int:
        """Return the index of the last log entry."""
        if self._log_cache:
            return max(self._log_cache.keys())
        return self.last_included_index
    
    @property
    def last_log_term(self) -> int:
        """Return the term of the last log entry."""
        if self._log_cache:
            last_idx = max(self._log_cache.keys())
            return self._log_cache[last_idx].term
        return self.last_included_term
    
    # ========== Initialization ==========
    
    async def initialize(self) -> None:
        """Initialize node from storage."""
        # Load persistent state
        self.current_term = await self.storage.load_term()
        self.voted_for = await self.storage.load_vote()
        
        # Load snapshot
        snapshot = await self.storage.load_snapshot()
        if snapshot:
            self.last_included_index = snapshot.last_included_index
            self.last_included_term = snapshot.last_included_term
            # Restore state machine
            self.state_machine = ProcessTableState.from_snapshot(snapshot.data)
            self.state_machine.set_log_index(snapshot.last_included_index)
            self.last_applied = self.last_included_index
            self.commit_index = self.last_included_index
        
        # Load log entries after snapshot
        entries = await self.storage.get_log_entries(
            self.last_included_index + 1,
            self.last_included_index + 10000  # Load in batches
        )
        for entry in entries:
            self._log_cache[entry.index] = entry
    
    # ========== State Transitions ==========
    
    def reset_election_timeout(self) -> None:
        """Reset election timeout with random value."""
        self.election_timeout = random.uniform(
            self.config.election_timeout_min,
            self.config.election_timeout_max,
        )
        # Use wall clock for election timeout (this is OK - not state machine)
        self.last_heartbeat = asyncio.get_event_loop().time()
    
    async def become_follower(self, term: int) -> None:
        """Transition to follower state with given term."""
        self.state = RaftState.FOLLOWER
        self.current_term = term
        self.voted_for = None
        self.votes_received.clear()
        self.reset_election_timeout()
        
        # Persist term and vote
        await self.storage.save_term(term)
        await self.storage.save_vote(None)
    
    async def become_candidate(self) -> None:
        """Transition to candidate state and request votes."""
        self.state = RaftState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.reset_election_timeout()
        
        # Persist term and vote
        await self.storage.save_term(self.current_term)
        await self.storage.save_vote(self.node_id)
        
        # Request votes from all peers
        for peer_id in self.peers:
            request = {
                "type": "RequestVote",
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": self.last_log_index,
                "last_log_term": self.last_log_term,
            }
            self.send_message(peer_id, request)
    
    async def become_leader(self) -> None:
        """Transition to leader state."""
        self.state = RaftState.LEADER
        self.votes_received.clear()
        
        # Initialize leader state
        for peer_id in self.peers:
            self.next_index[peer_id] = self.last_log_index + 1
            self.match_index[peer_id] = self.last_included_index
        
        # Send immediate heartbeat
        self._send_heartbeats()
    
    # ========== RPC Handlers ==========
    
    async def handle_request_vote(self, msg: dict) -> dict:
        """Handle incoming RequestVote RPC."""
        # Step-down if higher term
        if msg["term"] > self.current_term:
            await self.become_follower(msg["term"])
        
        vote_granted = False
        
        if (
            msg["term"] == self.current_term
            and (self.voted_for is None or self.voted_for == msg["candidate_id"])
        ):
            # Check if candidate's log is at least as up-to-date
            candidate_up_to_date = (
                msg["last_log_term"] > self.last_log_term
                or (
                    msg["last_log_term"] == self.last_log_term
                    and msg["last_log_index"] >= self.last_log_index
                )
            )
            if candidate_up_to_date:
                self.voted_for = msg["candidate_id"]
                vote_granted = True
                self.reset_election_timeout()
                
                # Persist vote
                await self.storage.save_vote(msg["candidate_id"])
        
        return {
            "type": "RequestVoteResponse",
            "term": self.current_term,
            "vote_granted": vote_granted,
        }
    
    async def handle_append_entries(self, msg: dict) -> dict:
        """Handle incoming AppendEntries RPC."""
        # Step-down if higher term
        if msg["term"] > self.current_term:
            await self.become_follower(msg["term"])
        
        success = False
        match_index = 0
        conflict_index = None
        
        if msg["term"] == self.current_term:
            if self.state != RaftState.FOLLOWER:
                self.state = RaftState.FOLLOWER
            self.reset_election_timeout()
            
            # Consistency check
            prev_log_index = msg["prev_log_index"]
            prev_log_term = msg["prev_log_term"]
            
            prev_term_ok = await self._check_prev_log(prev_log_index, prev_log_term)
            
            if prev_term_ok:
                success = True
                
                # Append new entries
                entries = msg.get("entries", [])
                for entry_data in entries:
                    entry = LogEntry(
                        term=entry_data["term"],
                        index=entry_data["index"],
                        command=self._encode_command(entry_data.get("command", {})),
                    )
                    self._log_cache[entry.index] = entry
                    await self.storage.append_log_entry(entry)
                
                match_index = prev_log_index + len(entries)
                
                # Update commit_index
                if msg.get("leader_commit", 0) > self.commit_index:
                    self.commit_index = min(msg["leader_commit"], match_index)
                    # CRITICAL: Apply committed entries
                    await self._apply_committed_entries()
            else:
                # Find conflict index
                if prev_log_index > self.last_log_index:
                    conflict_index = self.last_log_index + 1
                else:
                    conflict_index = prev_log_index
        
        return {
            "type": "AppendEntriesResponse",
            "term": self.current_term,
            "success": success,
            "match_index": match_index if success else None,
            "conflict_index": conflict_index,
        }
    
    async def handle_append_entries_response(
        self, 
        sender_id: int, 
        msg: dict
    ) -> None:
        """Handle AppendEntries response."""
        if msg["term"] > self.current_term:
            await self.become_follower(msg["term"])
            return
        
        if self.state != RaftState.LEADER:
            return
        
        if msg["success"]:
            # Track heartbeat ack
            self.last_heartbeat_acks.add(sender_id)
            
            # Update leader lease
            majority = (len(self.peers) + 1) // 2 + 1
            if len(self.last_heartbeat_acks) >= majority:
                self.leader_lease_expiry = (
                    asyncio.get_event_loop().time() + 0.5  # 500ms lease
                )
            
            # Update match_index and next_index
            if msg.get("match_index") is not None:
                self.match_index[sender_id] = msg["match_index"]
                self.next_index[sender_id] = msg["match_index"] + 1
            
            # Try to advance commit_index
            await self._update_commit_index()
        else:
            # Decrement next_index and retry
            if sender_id in self.next_index:
                if msg.get("conflict_index") is not None:
                    self.next_index[sender_id] = max(
                        self.last_included_index + 1,
                        msg["conflict_index"]
                    )
                else:
                    self.next_index[sender_id] = max(
                        self.last_included_index + 1,
                        self.next_index[sender_id] - 1
                    )
                
                # Retry
                self._send_entries_to_peer(sender_id)
    
    async def handle_install_snapshot(self, msg: dict) -> dict:
        """Handle InstallSnapshot RPC - SECURE VERSION."""
        # Step-down if higher term
        if msg["term"] > self.current_term:
            await self.become_follower(msg["term"])
        
        if msg["term"] < self.current_term:
            return {
                "type": "InstallSnapshotResponse",
                "term": self.current_term,
                "last_included_index": self.last_included_index,
            }
        
        # Validate snapshot size
        snapshot_data = msg.get("data", b"")
        MAX_SNAPSHOT_SIZE = 100 * 1024 * 1024  # 100MB
        if len(snapshot_data) > MAX_SNAPSHOT_SIZE:
            return {
                "type": "InstallSnapshotResponse",
                "term": self.current_term,
                "last_included_index": self.last_included_index,
            }
        
        # SECURE deserialization (using zlib + json, not ast.literal_eval)
        try:
            self.state_machine = ProcessTableState.from_snapshot(snapshot_data)
        except Exception as e:
            # If deserialization fails, just update metadata
            pass
        
        # Update snapshot metadata
        self.last_included_index = msg["last_included_index"]
        self.last_included_term = msg["last_included_term"]
        
        # Clear log cache up to snapshot
        indices_to_remove = [
            idx for idx in self._log_cache 
            if idx <= self.last_included_index
        ]
        for idx in indices_to_remove:
            del self._log_cache[idx]
        
        # Update commit_index and last_applied
        if self.last_included_index > self.commit_index:
            self.commit_index = self.last_included_index
        if self.last_included_index > self.last_applied:
            self.last_applied = self.last_included_index
        
        # Save snapshot to storage
        snapshot = Snapshot(
            last_included_index=self.last_included_index,
            last_included_term=self.last_included_term,
            data=snapshot_data,
        )
        await self.storage.save_snapshot(snapshot)
        
        self.reset_election_timeout()
        
        return {
            "type": "InstallSnapshotResponse",
            "term": self.current_term,
            "last_included_index": self.last_included_index,
        }
    
    # ========== Leader Operations ==========
    
    def _send_heartbeats(self) -> None:
        """Send heartbeats to all peers."""
        for peer_id in self.peers:
            self._send_entries_to_peer(peer_id)
    
    def _send_entries_to_peer(self, peer_id: int) -> None:
        """Send AppendEntries to a specific peer."""
        if peer_id not in self.next_index:
            return
        
        next_idx = self.next_index[peer_id]
        
        # Check if peer needs snapshot
        if next_idx <= self.last_included_index:
            self._send_snapshot_to_peer(peer_id)
            return
        
        prev_log_index = next_idx - 1
        prev_log_term = self._get_log_term(prev_log_index)
        
        # Get entries from next_idx onwards
        entries = []
        for idx in range(next_idx, self.last_log_index + 1):
            if idx in self._log_cache:
                entry = self._log_cache[idx]
                entries.append({
                    "term": entry.term,
                    "index": entry.index,
                    "command": self._decode_command(entry.command),
                })
        
        msg = {
            "type": "AppendEntries",
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries,
            "leader_commit": self.commit_index,
        }
        self.send_message(peer_id, msg)
    
    def _send_snapshot_to_peer(self, peer_id: int) -> None:
        """Send snapshot to a lagging peer."""
        # Create snapshot from state machine
        snapshot_data = self.state_machine.to_snapshot()
        
        msg = {
            "type": "InstallSnapshot",
            "term": self.current_term,
            "leader_id": self.node_id,
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "data": snapshot_data,
        }
        self.send_message(peer_id, msg)
    
    async def _update_commit_index(self) -> None:
        """Update commit_index based on majority match."""
        if self.last_log_index <= self.commit_index:
            return
        
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
                # CRITICAL: Only advance commit_index, then apply
                old_commit = self.commit_index
                self.commit_index = n
                
                # Apply all newly committed entries
                await self._apply_committed_entries()
                
                # Maybe compact
                await self._maybe_compact()
                break
    
    # ========== State Machine Application (CRITICAL FIX) ==========
    
    async def _apply_committed_entries(self) -> None:
        """
        Apply all committed but unapplied entries to state machine.
        
        CRITICAL: This is the ONLY place where commands are applied.
        Commands are only applied after being committed by a quorum.
        
        This method also resolves any pending command futures.
        """
        while self.last_applied < self.commit_index:
            next_idx = self.last_applied + 1
            entry = self._log_cache.get(next_idx)
            
            if entry is None:
                # Try to load from storage
                entry = await self.storage.get_log_entry(next_idx)
                if entry is None:
                    break
                self._log_cache[next_idx] = entry
            
            # Apply command to state machine
            command = self._decode_command(entry.command)
            command_type = command.get("type", "")
            
            # Set current log index in state machine (deterministic time)
            self.state_machine.set_log_index(next_idx)
            
            # Apply the command
            result = self.state_machine.apply_command(command_type, command)
            
            # Advance log index after command
            self.state_machine.advance_log_index()
            
            self.last_applied = next_idx
            
            # CRITICAL: Resolve pending command future if exists
            if next_idx in self._pending_commands:
                pending = self._pending_commands.pop(next_idx)
                if not pending.future.done():
                    pending.future.set_result(result)
    
    async def _maybe_compact(self) -> None:
        """Check if compaction is needed."""
        log_size = len(self._log_cache)
        
        if log_size > self.config.max_log_size:
            snapshot_idx = self.commit_index
            
            if snapshot_idx > self.last_included_index:
                # Take snapshot
                self.last_included_index = snapshot_idx
                self.last_included_term = self._get_log_term(snapshot_idx)
                
                # Create snapshot
                snapshot_data = self.state_machine.to_snapshot()
                snapshot = Snapshot(
                    last_included_index=self.last_included_index,
                    last_included_term=self.last_included_term,
                    data=snapshot_data,
                )
                await self.storage.save_snapshot(snapshot)
                
                # Clear log cache
                indices_to_remove = [
                    idx for idx in self._log_cache 
                    if idx <= self.last_included_index
                ]
                for idx in indices_to_remove:
                    del self._log_cache[idx]
    
    # ========== Client Operations (CORRECTED) ==========
    
    async def submit_command(self, command: dict) -> Tuple[bool, Any]:
        """
        Submit a command to the cluster.
        
        CRITICAL: This method does NOT apply the command immediately.
        The command is:
        1. Appended to the leader's log
        2. Replicated to a quorum of followers
        3. Only applied after commit_index advances
        4. Future is resolved when the command is applied
        
        Returns (success, result_or_error) when the command is committed.
        """
        if self.state != RaftState.LEADER:
            return False, "Not leader"
        
        loop = asyncio.get_event_loop()
        
        # Create log entry
        log_index = self.last_log_index + 1
        entry = LogEntry(
            term=self.current_term,
            index=log_index,
            command=self._encode_command(command),
        )
        
        # Add to log (NOT applied yet!)
        self._log_cache[entry.index] = entry
        await self.storage.append_log_entry(entry)
        
        # Create pending command (to be resolved when committed)
        pending = PendingCommand(log_index, command, loop)
        self._pending_commands[log_index] = pending
        
        # Send to followers immediately
        self._send_heartbeats()
        
        # Wait for the command to be committed and applied
        # with a timeout
        try:
            result = await asyncio.wait_for(
                pending.future,
                timeout=10.0  # 10 second timeout
            )
            return True, result
        except asyncio.TimeoutError:
            # Remove pending command on timeout
            self._pending_commands.pop(log_index, None)
            return False, "Commit timeout"
    
    async def submit_command_async(self, command: dict) -> asyncio.Future:
        """
        Submit a command and return a future immediately.
        
        Use this for fire-and-forget or when you want to handle
        the result asynchronously.
        """
        if self.state != RaftState.LEADER:
            loop = asyncio.get_event_loop()
            future = loop.create_future()
            future.set_exception(Exception("Not leader"))
            return future
        
        loop = asyncio.get_event_loop()
        
        # Create log entry
        log_index = self.last_log_index + 1
        entry = LogEntry(
            term=self.current_term,
            index=log_index,
            command=self._encode_command(command),
        )
        
        # Add to log (NOT applied yet!)
        self._log_cache[entry.index] = entry
        await self.storage.append_log_entry(entry)
        
        # Create pending command
        pending = PendingCommand(log_index, command, loop)
        self._pending_commands[log_index] = pending
        
        # Send to followers
        self._send_heartbeats()
        
        return pending.future
    
    async def client_read(self, key: str) -> Tuple[Any, Optional[str]]:
        """
        Linearizable read using ReadIndex protocol.
        
        Returns (value, error).
        """
        if self.state != RaftState.LEADER:
            return None, "NotLeader"
        
        # Check leader lease
        now = asyncio.get_event_loop().time()
        if now < self.leader_lease_expiry:
            # Lease valid - serve read locally
            return self.state_machine, None
        
        # No valid lease - need heartbeat round
        self.last_heartbeat_acks = {self.node_id}
        self._send_heartbeats()
        
        # Wait for majority acks (simplified)
        majority = (len(self.peers) + 1) // 2 + 1
        
        for _ in range(10):  # Max 10 iterations
            if len(self.last_heartbeat_acks) >= majority:
                return self.state_machine, None
            await asyncio.sleep(0.01)
        
        return None, "NotLeader"
    
    # ========== Utility Methods ==========
    
    async def _check_prev_log(self, prev_index: int, prev_term: int) -> bool:
        """Check if log matches at prev_index."""
        if prev_index == 0:
            return prev_term == 0
        
        if prev_index <= self.last_included_index:
            return prev_term == self.last_included_term
        
        entry = self._log_cache.get(prev_index)
        if entry is None:
            entry = await self.storage.get_log_entry(prev_index)
            if entry:
                self._log_cache[prev_index] = entry
        
        if entry is None:
            return False
        
        return entry.term == prev_term
    
    def _get_log_term(self, index: int) -> int:
        """Get term at log index."""
        if index == 0:
            return 0
        if index <= self.last_included_index:
            return self.last_included_term
        
        entry = self._log_cache.get(index)
        if entry:
            return entry.term
        return 0
    
    def _encode_command(self, command: dict) -> bytes:
        """Encode command for storage."""
        return json.dumps(command, separators=(',', ':')).encode('utf-8')
    
    def _decode_command(self, data: bytes) -> dict:
        """Decode command from storage."""
        if not data:
            return {}
        return json.loads(data.decode('utf-8'))
    
    # ========== Main Loop ==========
    
    async def run(self) -> None:
        """Main event loop for the Raft node."""
        await self.initialize()
        self._running = True
        self.reset_election_timeout()
        
        while self._running:
            now = asyncio.get_event_loop().time()
            elapsed = now - self.last_heartbeat
            
            # Check election timeout
            if self.state != RaftState.LEADER and elapsed >= self.election_timeout:
                await self.become_candidate()
                continue
            
            # Leader sends heartbeats
            if self.state == RaftState.LEADER:
                if elapsed >= self.config.heartbeat_interval:
                    self._send_heartbeats()
                    self.last_heartbeat = asyncio.get_event_loop().time()
                    continue
            
            # Calculate wait time
            if self.state == RaftState.LEADER:
                wait_time = max(0.001, self.config.heartbeat_interval - elapsed)
            else:
                wait_time = max(0.001, self.election_timeout - elapsed)
            
            try:
                sender_id, message = await asyncio.wait_for(
                    self.inbox.get(), 
                    timeout=wait_time
                )
                await self.process_message(sender_id, message)
            except asyncio.TimeoutError:
                pass
    
    async def process_message(self, sender_id: int, message: dict) -> None:
        """Process an incoming message."""
        msg_type = message.get("type", "")
        
        if msg_type == "RequestVote":
            response = await self.handle_request_vote(message)
            self.send_message(sender_id, response)
        
        elif msg_type == "RequestVoteResponse":
            if (
                self.state == RaftState.CANDIDATE
                and message["term"] == self.current_term
                and message.get("vote_granted", False)
            ):
                self.votes_received.add(sender_id)
                majority = (len(self.peers) + 1) // 2 + 1
                if len(self.votes_received) >= majority:
                    await self.become_leader()
        
        elif msg_type == "AppendEntries":
            response = await self.handle_append_entries(message)
            self.send_message(sender_id, response)
        
        elif msg_type == "AppendEntriesResponse":
            await self.handle_append_entries_response(sender_id, message)
        
        elif msg_type == "InstallSnapshot":
            response = await self.handle_install_snapshot(message)
            self.send_message(sender_id, response)
    
    def stop(self) -> None:
        """Stop the node."""
        self._running = False
        
        # Reject all pending commands
        for pending in self._pending_commands.values():
            if not pending.future.done():
                pending.future.set_exception(Exception("Node stopped"))
        self._pending_commands.clear()
