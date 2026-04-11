"""AetherGrid Core - Consensus Layer Models.

Defines the core data structures for the Raft consensus algorithm.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, List, Optional


class RaftState(Enum):
    """Raft node states."""

    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """A single log entry."""

    term: int
    command: Any


@dataclass
class RequestVote:
    """RequestVote RPC message."""

    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    """Response to RequestVote RPC."""

    term: int
    vote_granted: bool


@dataclass
class AppendEntries:
    """AppendEntries RPC message (also used for heartbeats)."""

    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: List[dict] = field(default_factory=list)
    leader_commit: int = 0


@dataclass
class AppendEntriesResponse:
    """Response to AppendEntries RPC."""

    term: int
    success: bool
    match_index: Optional[int] = None
    conflict_index: Optional[int] = None  # For fast roll-back on rejection


@dataclass
class InstallSnapshotRequest:
    """InstallSnapshot RPC message."""

    term: int
    leader_id: int
    last_included_index: int
    last_included_term: int
    data: bytes  # Snapshot data (serialized state machine)
