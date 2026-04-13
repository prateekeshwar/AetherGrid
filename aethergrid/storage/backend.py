"""AetherGrid v2.0 - Storage Backend Abstraction.

Provides an abstract interface for storage backends with implementations:
- SQLiteWALStorage: Production-ready SQLite with WAL mode
- MemoryStorage: In-memory storage for testing

Key features:
- O(1) log append operations
- Atomic writes with proper fsync
- Snapshot support
- Column-family-like organization
"""

import abc
import asyncio
import json
import os
import sqlite3
import struct
import threading
import zlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union


class StorageError(Exception):
    """Base exception for storage errors."""
    pass


class SnapshotTooLargeError(StorageError):
    """Snapshot exceeds maximum allowed size."""
    pass


@dataclass
class LogEntry:
    """A single log entry."""
    term: int
    index: int
    command: bytes  # Serialized command
    
    def to_dict(self) -> dict:
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command.hex() if self.command else "",
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "LogEntry":
        return cls(
            term=data["term"],
            index=data["index"],
            command=bytes.fromhex(data["command"]) if data.get("command") else b"",
        )


@dataclass
class Snapshot:
    """Snapshot of state machine."""
    last_included_index: int
    last_included_term: int
    data: bytes
    
    def to_dict(self) -> dict:
        return {
            "last_included_index": self.last_included_index,
            "last_included_term": self.last_included_term,
            "data": self.data.hex() if self.data else "",
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Snapshot":
        return cls(
            last_included_index=data["last_included_index"],
            last_included_term=data["last_included_term"],
            data=bytes.fromhex(data["data"]) if data.get("data") else b"",
        )


class StorageBackend(abc.ABC):
    """Abstract base class for storage backends."""
    
    @abc.abstractmethod
    async def save_term(self, term: int) -> None:
        """Persist current term."""
        pass
    
    @abc.abstractmethod
    async def load_term(self) -> int:
        """Load current term."""
        pass
    
    @abc.abstractmethod
    async def save_vote(self, voted_for: Optional[int]) -> None:
        """Persist vote."""
        pass
    
    @abc.abstractmethod
    async def load_vote(self) -> Optional[int]:
        """Load vote."""
        pass
    
    @abc.abstractmethod
    async def append_log_entry(self, entry: LogEntry) -> None:
        """Append a log entry."""
        pass
    
    @abc.abstractmethod
    async def get_log_entry(self, index: int) -> Optional[LogEntry]:
        """Get log entry by index."""
        pass
    
    @abc.abstractmethod
    async def get_log_entries(self, start: int, end: int) -> List[LogEntry]:
        """Get log entries in range [start, end)."""
        pass
    
    @abc.abstractmethod
    async def truncate_log(self, from_index: int) -> None:
        """Truncate log from index onwards."""
        pass
    
    @abc.abstractmethod
    async def get_last_log_index(self) -> int:
        """Get last log index."""
        pass
    
    @abc.abstractmethod
    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save snapshot."""
        pass
    
    @abc.abstractmethod
    async def load_snapshot(self) -> Optional[Snapshot]:
        """Load snapshot."""
        pass
    
    @abc.abstractmethod
    async def close(self) -> None:
        """Close storage."""
        pass
    
    # Convenience methods for state machine data
    
    async def save_state_machine(self, state: dict) -> None:
        """Save state machine state."""
        pass
    
    async def load_state_machine(self) -> dict:
        """Load state machine state."""
        return {}


class SQLiteWALStorage(StorageBackend):
    """
    Production-grade SQLite storage with WAL mode.
    
    Features:
    - O(1) log append (SQLite B-tree)
    - WAL mode for concurrent reads
    - Atomic transactions with fsync
    - Compressed snapshots
    """
    
    MAX_SNAPSHOT_SIZE = 100 * 1024 * 1024  # 100MB
    
    def __init__(self, node_id: int, data_dir: str = "data"):
        self.node_id = node_id
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.db_path = self.data_dir / f"node_{node_id}.db"
        self._lock = threading.Lock()
        self._conn: Optional[sqlite3.Connection] = None
        self._init_db()
    
    def _get_conn(self) -> sqlite3.Connection:
        """Get or create database connection (thread-safe)."""
        # Create a new connection per thread for thread safety
        # SQLite connections are not thread-safe by default
        conn = sqlite3.connect(
            str(self.db_path),
            isolation_level=None,  # Autocommit mode, we manage transactions
            check_same_thread=False,  # Allow use across threads
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        return conn
    
    def _init_db(self) -> None:
        """Initialize database schema."""
        conn = self._get_conn()
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raft_meta (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raft_log (
                idx INTEGER PRIMARY KEY,
                term INTEGER NOT NULL,
                command BLOB NOT NULL
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_included_index INTEGER NOT NULL,
                last_included_term INTEGER NOT NULL,
                data BLOB NOT NULL,
                compressed INTEGER NOT NULL DEFAULT 0
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS state_machine (
                key TEXT PRIMARY KEY,
                value BLOB NOT NULL
            )
        """)
        
        # Create indexes
        conn.execute("CREATE INDEX IF NOT EXISTS idx_log_term ON raft_log(term)")
    
    # ========== Raft Meta ==========
    
    async def save_term(self, term: int) -> None:
        """Persist current term."""
        def _save():
            conn = self._get_conn()
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT OR REPLACE INTO raft_meta (key, value) VALUES ('current_term', ?)",
                (struct.pack('>Q', term),)
            )
            conn.execute("COMMIT")
        
        await asyncio.get_event_loop().run_in_executor(None, _save)
    
    async def load_term(self) -> int:
        """Load current term."""
        def _load():
            conn = self._get_conn()
            cursor = conn.execute(
                "SELECT value FROM raft_meta WHERE key = 'current_term'"
            )
            row = cursor.fetchone()
            if row is None:
                return 0
            return struct.unpack('>Q', row[0])[0]
        
        return await asyncio.get_event_loop().run_in_executor(None, _load)
    
    async def save_vote(self, voted_for: Optional[int]) -> None:
        """Persist vote."""
        def _save():
            conn = self._get_conn()
            conn.execute("BEGIN IMMEDIATE")
            if voted_for is None:
                conn.execute(
                    "INSERT OR REPLACE INTO raft_meta (key, value) VALUES ('voted_for', ?)",
                    (b'',)
                )
            else:
                conn.execute(
                    "INSERT OR REPLACE INTO raft_meta (key, value) VALUES ('voted_for', ?)",
                    (struct.pack('>q', voted_for),)
                )
            conn.execute("COMMIT")
        
        await asyncio.get_event_loop().run_in_executor(None, _save)
    
    async def load_vote(self) -> Optional[int]:
        """Load vote."""
        def _load():
            conn = self._get_conn()
            cursor = conn.execute(
                "SELECT value FROM raft_meta WHERE key = 'voted_for'"
            )
            row = cursor.fetchone()
            if row is None or not row[0]:
                return None
            return struct.unpack('>q', row[0])[0]
        
        return await asyncio.get_event_loop().run_in_executor(None, _load)
    
    # ========== Raft Log ==========
    
    async def append_log_entry(self, entry: LogEntry) -> None:
        """Append a log entry - O(log n) due to B-tree."""
        def _append():
            conn = self._get_conn()
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT OR REPLACE INTO raft_log (idx, term, command) VALUES (?, ?, ?)",
                (entry.index, entry.term, entry.command)
            )
            conn.execute("COMMIT")
        
        await asyncio.get_event_loop().run_in_executor(None, _append)
    
    async def append_log_entries(self, entries: List[LogEntry]) -> None:
        """Append multiple log entries atomically."""
        def _append():
            conn = self._get_conn()
            conn.execute("BEGIN IMMEDIATE")
            for entry in entries:
                conn.execute(
                    "INSERT OR REPLACE INTO raft_log (idx, term, command) VALUES (?, ?, ?)",
                    (entry.index, entry.term, entry.command)
                )
            conn.execute("COMMIT")
        
        await asyncio.get_event_loop().run_in_executor(None, _append)
    
    async def get_log_entry(self, index: int) -> Optional[LogEntry]:
        """Get log entry by index."""
        def _get():
            conn = self._get_conn()
            cursor = conn.execute(
                "SELECT term, command FROM raft_log WHERE idx = ?",
                (index,)
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return LogEntry(term=row[0], index=index, command=row[1])
        
        return await asyncio.get_event_loop().run_in_executor(None, _get)
    
    async def get_log_entries(self, start: int, end: int) -> List[LogEntry]:
        """Get log entries in range [start, end)."""
        def _get():
            conn = self._get_conn()
            cursor = conn.execute(
                "SELECT idx, term, command FROM raft_log WHERE idx >= ? AND idx < ? ORDER BY idx",
                (start, end)
            )
            entries = []
            for row in cursor:
                entries.append(LogEntry(
                    index=row[0],
                    term=row[1],
                    command=row[2]
                ))
            return entries
        
        return await asyncio.get_event_loop().run_in_executor(None, _get)
    
    async def truncate_log(self, from_index: int) -> None:
        """Truncate log from index onwards."""
        def _truncate():
            conn = self._get_conn()
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM raft_log WHERE idx >= ?", (from_index,))
            conn.execute("COMMIT")
        
        await asyncio.get_event_loop().run_in_executor(None, _truncate)
    
    async def get_last_log_index(self) -> int:
        """Get last log index."""
        def _get():
            conn = self._get_conn()
            cursor = conn.execute("SELECT MAX(idx) FROM raft_log")
            row = cursor.fetchone()
            if row[0] is None:
                return 0
            return row[0]
        
        return await asyncio.get_event_loop().run_in_executor(None, _get)
    
    async def get_last_log_term(self) -> int:
        """Get term of last log entry."""
        def _get():
            conn = self._get_conn()
            cursor = conn.execute("SELECT term FROM raft_log ORDER BY idx DESC LIMIT 1")
            row = cursor.fetchone()
            if row is None:
                return 0
            return row[0]
        
        return await asyncio.get_event_loop().run_in_executor(None, _get)
    
    # ========== Snapshot ==========
    
    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save snapshot with compression."""
        if len(snapshot.data) > self.MAX_SNAPSHOT_SIZE:
            raise SnapshotTooLargeError(
                f"Snapshot size {len(snapshot.data)} exceeds max {self.MAX_SNAPSHOT_SIZE}"
            )
        
        def _save():
            conn = self._get_conn()
            
            # Compress snapshot data
            compressed_data = zlib.compress(snapshot.data, level=6)
            is_compressed = 1
            
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("""
                INSERT OR REPLACE INTO snapshots 
                (id, last_included_index, last_included_term, data, compressed)
                VALUES (1, ?, ?, ?, ?)
            """, (
                snapshot.last_included_index,
                snapshot.last_included_term,
                compressed_data,
                is_compressed
            ))
            
            # Truncate log entries before snapshot
            conn.execute(
                "DELETE FROM raft_log WHERE idx <= ?",
                (snapshot.last_included_index,)
            )
            
            conn.execute("COMMIT")
        
        await asyncio.get_event_loop().run_in_executor(None, _save)
    
    async def load_snapshot(self) -> Optional[Snapshot]:
        """Load snapshot with decompression."""
        def _load():
            conn = self._get_conn()
            cursor = conn.execute("""
                SELECT last_included_index, last_included_term, data, compressed
                FROM snapshots WHERE id = 1
            """)
            row = cursor.fetchone()
            if row is None:
                return None
            
            last_included_index, last_included_term, data, compressed = row
            
            if compressed:
                data = zlib.decompress(data)
            
            return Snapshot(
                last_included_index=last_included_index,
                last_included_term=last_included_term,
                data=data
            )
        
        return await asyncio.get_event_loop().run_in_executor(None, _load)
    
    # ========== State Machine ==========
    
    async def save_state_machine(self, state: dict) -> None:
        """Save state machine state."""
        def _save():
            conn = self._get_conn()
            data = json.dumps(state, separators=(',', ':')).encode('utf-8')
            compressed = zlib.compress(data, level=6)
            
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT OR REPLACE INTO state_machine (key, value) VALUES ('state', ?)",
                (compressed,)
            )
            conn.execute("COMMIT")
        
        await asyncio.get_event_loop().run_in_executor(None, _save)
    
    async def load_state_machine(self) -> dict:
        """Load state machine state."""
        def _load():
            conn = self._get_conn()
            cursor = conn.execute("SELECT value FROM state_machine WHERE key = 'state'")
            row = cursor.fetchone()
            if row is None:
                return {}
            
            data = zlib.decompress(row[0])
            return json.loads(data.decode('utf-8'))
        
        return await asyncio.get_event_loop().run_in_executor(None, _load)
    
    # ========== Utility ==========
    
    async def close(self) -> None:
        """Close database connection."""
        # With check_same_thread=False, we don't need to track connections
        # SQLite will handle cleanup when the object is garbage collected
        pass
    
    async def compact(self) -> None:
        """Run VACUUM to compact database."""
        def _compact():
            conn = self._get_conn()
            conn.execute("VACUUM")
        
        await asyncio.get_event_loop().run_in_executor(None, _compact)
    
    def get_stats(self) -> dict:
        """Get storage statistics."""
        conn = self._get_conn()
        
        # Log count
        cursor = conn.execute("SELECT COUNT(*) FROM raft_log")
        log_count = cursor.fetchone()[0]
        
        # Database size
        db_size = os.path.getsize(self.db_path) if self.db_path.exists() else 0
        
        # WAL size
        wal_path = Path(str(self.db_path) + "-wal")
        wal_size = wal_path.stat().st_size if wal_path.exists() else 0
        
        return {
            "log_entries": log_count,
            "db_size_bytes": db_size,
            "wal_size_bytes": wal_size,
        }


class MemoryStorage(StorageBackend):
    """
    In-memory storage for testing.
    
    NOT production-ready - data is lost on restart.
    """
    
    def __init__(self):
        self._term: int = 0
        self._vote: Optional[int] = None
        self._log: Dict[int, LogEntry] = {}
        self._snapshot: Optional[Snapshot] = None
        self._state_machine: dict = {}
        self._lock = threading.Lock()
    
    async def save_term(self, term: int) -> None:
        with self._lock:
            self._term = term
    
    async def load_term(self) -> int:
        with self._lock:
            return self._term
    
    async def save_vote(self, voted_for: Optional[int]) -> None:
        with self._lock:
            self._vote = voted_for
    
    async def load_vote(self) -> Optional[int]:
        with self._lock:
            return self._vote
    
    async def append_log_entry(self, entry: LogEntry) -> None:
        with self._lock:
            self._log[entry.index] = entry
    
    async def get_log_entry(self, index: int) -> Optional[LogEntry]:
        with self._lock:
            return self._log.get(index)
    
    async def get_log_entries(self, start: int, end: int) -> List[LogEntry]:
        with self._lock:
            return [
                self._log[i] 
                for i in range(start, end) 
                if i in self._log
            ]
    
    async def truncate_log(self, from_index: int) -> None:
        with self._lock:
            indices_to_remove = [i for i in self._log if i >= from_index]
            for i in indices_to_remove:
                del self._log[i]
    
    async def get_last_log_index(self) -> int:
        with self._lock:
            if not self._log:
                return self._snapshot.last_included_index if self._snapshot else 0
            return max(self._log.keys())
    
    async def get_last_log_term(self) -> int:
        with self._lock:
            if not self._log:
                return self._snapshot.last_included_term if self._snapshot else 0
            last_idx = max(self._log.keys())
            return self._log[last_idx].term
    
    async def save_snapshot(self, snapshot: Snapshot) -> None:
        with self._lock:
            self._snapshot = snapshot
            # Truncate log
            indices_to_remove = [i for i in self._log if i <= snapshot.last_included_index]
            for i in indices_to_remove:
                del self._log[i]
    
    async def load_snapshot(self) -> Optional[Snapshot]:
        with self._lock:
            return self._snapshot
    
    async def save_state_machine(self, state: dict) -> None:
        with self._lock:
            self._state_machine = state.copy()
    
    async def load_state_machine(self) -> dict:
        with self._lock:
            return self._state_machine.copy()
    
    async def close(self) -> None:
        pass
