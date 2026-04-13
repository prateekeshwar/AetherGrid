# AetherGrid v2.0 Implementation Guide

## Quick Start: Critical Fixes

This guide covers the immediate changes needed to address the critical flaws identified in the feasibility analysis.

---

## 1. Replace Storage Layer (Priority: CRITICAL)

### Current Problem
```python
# storage.py - O(n) full file rewrites
def _sync_append() -> None:
    log_data = []
    if os.path.exists(self.log_file):
        with open(self.log_file, "r") as f:
            log_data = json.load(f).get("entries", [])
    # ... rewrite entire file
```

### Solution: RocksDB Backend

```python
# storage_v2.py

import rocksdb
import struct
from typing import Optional, List, Tuple
from dataclasses import dataclass
from pathlib import Path

class RaftStorageV2:
    """Production-grade Raft storage using RocksDB."""
    
    def __init__(self, node_id: int, data_dir: str = "data"):
        self.node_id = node_id
        self.db_path = Path(data_dir) / f"node_{node_id}"
        self.db_path.mkdir(parents=True, exist_ok=True)
        
        # Configure RocksDB options
        opts = rocksdb.Options()
        opts.create_if_missing = True
        opts.write_buffer_size = 64 * 1024 * 1024  # 64MB memtable
        opts.max_write_buffer_number = 3
        opts.target_file_size_base = 64 * 1024 * 1024
        opts.compression = rocksdb.CompressionType.lz4_compression
        
        # Column families for different data types
        self.cf_options = {
            'default': rocksdb.ColumnFamilyOptions(),
            'log': rocksdb.ColumnFamilyOptions(),
            'meta': rocksdb.ColumnFamilyOptions(),
        }
        
        opts.column_families = [
            rocksdb.ColumnFamilyDescriptor(b'default', self.cf_options['default']),
            rocksdb.ColumnFamilyDescriptor(b'log', self.cf_options['log']),
            rocksdb.ColumnFamilyDescriptor(b'meta', self.cf_options['meta']),
        ]
        
        self.db = rocksdb.DB(str(self.db_path), opts)
        
        # Get column family handles
        self.cf_log = self.db.get_column_family(b'log')
        self.cf_meta = self.db.get_column_family(b'meta')
    
    # ========== Meta Operations ==========
    
    def save_term(self, term: int) -> None:
        """Persist current term - O(1)."""
        self.db.put(b'current_term', struct.pack('>Q', term), 
                   column_family=self.cf_meta)
    
    def load_term(self) -> int:
        """Load current term."""
        val = self.db.get(b'current_term', column_family=self.cf_meta)
        return struct.unpack('>Q', val)[0] if val else 0
    
    def save_vote(self, voted_for: Optional[int]) -> None:
        """Persist vote."""
        if voted_for is None:
            self.db.put(b'voted_for', b'', column_family=self.cf_meta)
        else:
            self.db.put(b'voted_for', struct.pack('>q', voted_for),
                       column_family=self.cf_meta)
    
    def load_vote(self) -> Optional[int]:
        """Load vote."""
        val = self.db.get(b'voted_for', column_family=self.cf_meta)
        if not val:
            return None
        return struct.unpack('>q', val)[0]
    
    # ========== Log Operations ==========
    
    def append_log_entry(self, index: int, entry: bytes) -> None:
        """Append log entry - O(1)."""
        key = struct.pack('>Q', index)
        self.db.put(key, entry, column_family=self.cf_log)
    
    def get_log_entry(self, index: int) -> Optional[bytes]:
        """Get log entry by index."""
        key = struct.pack('>Q', index)
        return self.db.get(key, column_family=self.cf_log)
    
    def get_log_range(self, start: int, end: int) -> List[Tuple[int, bytes]]:
        """Get log entries in range [start, end)."""
        entries = []
        start_key = struct.pack('>Q', start)
        
        it = self.db.iteritems(column_family=self.cf_log)
        it.seek(start_key)
        
        for key, value in it:
            idx = struct.unpack('>Q', key)[0]
            if idx >= end:
                break
            entries.append((idx, value))
        
        return entries
    
    def truncate_log_from(self, index: int) -> None:
        """Truncate log from index onwards."""
        batch = rocksdb.WriteBatch()
        start_key = struct.pack('>Q', index)
        
        it = self.db.iterkeys(column_family=self.cf_log)
        it.seek(start_key)
        
        for key in it:
            batch.delete(key, column_family=self.cf_log)
        
        self.db.write(batch)
    
    def get_last_log_index(self) -> int:
        """Get last log index."""
        it = self.db.iterkeys(column_family=self.cf_log, reverse=True)
        it.seek_to_last()
        try:
            key = next(it)
            return struct.unpack('>Q', key)[0]
        except StopIteration:
            return 0
    
    # ========== Snapshot Operations ==========
    
    def save_snapshot(
        self,
        last_included_index: int,
        last_included_term: int,
        snapshot_data: bytes,
    ) -> None:
        """Save snapshot atomically."""
        batch = rocksdb.WriteBatch()
        
        # Save metadata
        batch.put(b'snapshot_index', struct.pack('>Q', last_included_index),
                 column_family=self.cf_meta)
        batch.put(b'snapshot_term', struct.pack('>Q', last_included_term),
                 column_family=self.cf_meta)
        batch.put(b'snapshot_data', snapshot_data,
                 column_family=self.cf_meta)
        
        # Truncate log entries before snapshot
        end_key = struct.pack('>Q', last_included_index + 1)
        it = self.db.iterkeys(column_family=self.cf_log)
        it.seek(struct.pack('>Q', 1))
        
        for key in it:
            if key >= end_key:
                break
            batch.delete(key, column_family=self.cf_log)
        
        self.db.write(batch)
    
    def load_snapshot(self) -> Optional[Tuple[int, int, bytes]]:
        """Load snapshot."""
        idx = self.db.get(b'snapshot_index', column_family=self.cf_meta)
        term = self.db.get(b'snapshot_term', column_family=self.cf_meta)
        data = self.db.get(b'snapshot_data', column_family=self.cf_meta)
        
        if not idx or not term:
            return None
        
        return (
            struct.unpack('>Q', idx)[0],
            struct.unpack('>Q', term)[0],
            data,
        )
    
    # ========== Utility ==========
    
    def close(self) -> None:
        """Close database."""
        self.db.close()
```

---

## 2. Fix Security Vulnerability (Priority: CRITICAL)

### Current Problem
```python
# node.py:574-575 - CODE INJECTION RISK
import ast
snapshot_state = ast.literal_eval(msg.data.decode("utf-8"))
```

### Solution: Protobuf Serialization

```python
# serialization.py

from typing import Any
import json

# Maximum snapshot size (10MB)
MAX_SNAPSHOT_SIZE = 10 * 1024 * 1024

class SecureSerializer:
    """
    Secure serialization for snapshots.
    
    Uses JSON with strict validation - no code execution possible.
    For production, use Protocol Buffers.
    """
    
    @staticmethod
    def serialize_state_machine(state: dict) -> bytes:
        """Serialize state machine to JSON bytes."""
        return json.dumps(state, separators=(',', ':')).encode('utf-8')
    
    @staticmethod
    def deserialize_state_machine(data: bytes) -> dict:
        """
        Deserialize state machine from JSON bytes.
        
        Safe: JSON parser cannot execute code.
        """
        # Validate size
        if len(data) > MAX_SNAPSHOT_SIZE:
            raise ValueError(f"Snapshot too large: {len(data)} bytes")
        
        # Parse JSON (safe - no code execution)
        try:
            state = json.loads(data.decode('utf-8'))
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in snapshot: {e}")
        
        # Validate structure
        if not isinstance(state, dict):
            raise ValueError("Snapshot must be a dictionary")
        
        return state


# For production, use Protocol Buffers:
# 
# message Snapshot {
#     uint64 last_included_index = 1;
#     uint64 last_included_term = 2;
#     map<string, string> state_machine = 3;
# }
#
# snapshot = Snapshot()
# snapshot.ParseFromString(data)  # Safe - protobuf parser
```

### Updated Snapshot Handler

```python
# node_v2.py (updated handle_install_snapshot)

def handle_install_snapshot(self, msg: InstallSnapshotRequest) -> None:
    """Handle InstallSnapshot RPC - SECURE VERSION."""
    # Step-down if higher term
    if msg.term > self.current_term:
        self.become_follower(msg.term)
    
    if msg.term < self.current_term:
        return  # Ignore stale snapshot
    
    # Validate snapshot size
    if len(msg.data) > MAX_SNAPSHOT_SIZE:
        # Log warning but don't crash
        print(f"WARNING: Oversized snapshot rejected ({len(msg.data)} bytes)")
        return
    
    # SECURE deserialization (no code execution)
    try:
        snapshot_state = SecureSerializer.deserialize_state_machine(msg.data)
    except ValueError as e:
        print(f"WARNING: Invalid snapshot data: {e}")
        return
    
    # Install the snapshot
    self.state_machine = snapshot_state
    self.last_included_index = msg.last_included_index
    self.last_included_term = msg.last_included_term
    self.log = []
    
    if msg.last_included_index > self.commit_index:
        self.commit_index = msg.last_included_index
    if msg.last_included_index > self.last_applied:
        self.last_applied = msg.last_included_index
    
    self.reset_election_timeout()
```

---

## 3. Add Real Network Layer (Priority: HIGH)

### Current Problem
```python
# simulator.py - Uses in-memory queues, not real networking
self.inboxes[target_id].put_nowait((sender_id, message))
```

### Solution: gRPC Transport

```python
# transport/grpc_transport.py

import grpc
import asyncio
from typing import Callable, Dict
from concurrent import futures

import aethergrid_pb2
import aethergrid_pb2_grpc

class GrpcRaftTransport:
    """gRPC-based transport for Raft messages."""
    
    def __init__(self, node_id: int, port: int):
        self.node_id = node_id
        self.port = port
        self.channels: Dict[int, grpc.aio.Channel] = {}
        self.stubs: Dict[int, aethergrid_pb2_grpc.RaftServiceStub] = {}
        self.message_handler: Callable = None
    
    async def start(self) -> None:
        """Start the gRPC server."""
        self.server = grpc.aio.server(
            futures.ThreadPoolExecutor(max_workers=10),
        )
        
        aethergrid_pb2_grpc.add_RaftServiceServicer_to_server(
            RaftServicer(self), self.server
        )
        
        self.server.add_insecure_port(f'[::]:{self.port}')
        await self.server.start()
    
    async def stop(self) -> None:
        """Stop the gRPC server."""
        await self.server.stop(grace=5)
        
        for channel in self.channels.values():
            await channel.close()
    
    def set_message_handler(self, handler: Callable) -> None:
        """Set the handler for incoming messages."""
        self.message_handler = handler
    
    async def connect_to_peer(self, peer_id: int, address: str) -> None:
        """Connect to a peer node."""
        channel = grpc.aio.insecure_channel(address)
        self.channels[peer_id] = channel
        self.stubs[peer_id] = aethergrid_pb2_grpc.RaftServiceStub(channel)
    
    async def send_append_entries(
        self,
        peer_id: int,
        request: aethergrid_pb2.AppendEntriesRequest,
    ) -> aethergrid_pb2.AppendEntriesResponse:
        """Send AppendEntries RPC to peer."""
        stub = self.stubs.get(peer_id)
        if not stub:
            raise ValueError(f"Not connected to peer {peer_id}")
        
        return await stub.AppendEntries(request)
    
    async def send_request_vote(
        self,
        peer_id: int,
        request: aethergrid_pb2.RequestVoteRequest,
    ) -> aethergrid_pb2.RequestVoteResponse:
        """Send RequestVote RPC to peer."""
        stub = self.stubs.get(peer_id)
        if not stub:
            raise ValueError(f"Not connected to peer {peer_id}")
        
        return await stub.RequestVote(request)


class RaftServicer(aethergrid_pb2_grpc.RaftServiceServicer):
    """gRPC service implementation for Raft RPCs."""
    
    def __init__(self, transport: GrpcRaftTransport):
        self.transport = transport
    
    async def AppendEntries(
        self,
        request: aethergrid_pb2.AppendEntriesRequest,
        context: grpc.ServicerContext,
    ) -> aethergrid_pb2.AppendEntriesResponse:
        """Handle AppendEntries RPC."""
        if self.transport.message_handler:
            return await self.transport.message_handler('append_entries', request)
        
        # Default: not implemented
        return aethergrid_pb2.AppendEntriesResponse(
            term=0,
            success=False,
        )
    
    async def RequestVote(
        self,
        request: aethergrid_pb2.RequestVoteRequest,
        context: grpc.ServicerContext,
    ) -> aethergrid_pb2.RequestVoteResponse:
        """Handle RequestVote RPC."""
        if self.transport.message_handler:
            return await self.transport.message_handler('request_vote', request)
        
        return aethergrid_pb2.RequestVoteResponse(
            term=0,
            vote_granted=False,
        )
```

---

## 4. Add Observability (Priority: HIGH)

```python
# observability/metrics.py

from prometheus_client import Counter, Histogram, Gauge, Info, start_http_server

# Raft metrics
RAFT_TERM = Gauge('aethergrid_raft_term', 'Current Raft term', ['node_id'])
RAFT_STATE = Gauge(
    'aethergrid_raft_state', 
    'Current state (0=follower, 1=candidate, 2=leader)',
    ['node_id']
)
RAFT_COMMIT_INDEX = Gauge('aethergrid_raft_commit_index', 'Commit index', ['node_id'])
RAFT_LOG_LENGTH = Gauge('aethergrid_raft_log_length', 'Log entries', ['node_id'])

# Latency metrics
APPEND_ENTRIES_LATENCY = Histogram(
    'aethergrid_append_entries_latency_seconds',
    'AppendEntries latency',
    ['node_id'],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# Task metrics
TASKS_BY_STATUS = Gauge(
    'aethergrid_tasks_total',
    'Tasks by status',
    ['node_id', 'status']
)
TASK_OPERATIONS = Counter(
    'aethergrid_task_operations_total',
    'Task operations',
    ['node_id', 'operation', 'status']
)


class MetricsCollector:
    """Collects and exposes metrics for a Raft node."""
    
    def __init__(self, node_id: int, metrics_port: int = 9090):
        self.node_id = str(node_id)
        self.metrics_port = metrics_port
    
    def start(self) -> None:
        """Start Prometheus metrics server."""
        start_http_server(self.metrics_port)
    
    def update_raft_state(self, state: str, term: int, commit: int, log_len: int) -> None:
        """Update Raft state metrics."""
        state_map = {'follower': 0, 'candidate': 1, 'leader': 2}
        
        RAFT_STATE.labels(node_id=self.node_id).set(state_map.get(state, 0))
        RAFT_TERM.labels(node_id=self.node_id).set(term)
        RAFT_COMMIT_INDEX.labels(node_id=self.node_id).set(commit)
        RAFT_LOG_LENGTH.labels(node_id=self.node_id).set(log_len)
    
    def observe_append_entries(self, latency: float) -> None:
        """Record AppendEntries latency."""
        APPEND_ENTRIES_LATENCY.labels(node_id=self.node_id).observe(latency)
    
    def record_task_operation(self, operation: str, success: bool) -> None:
        """Record task operation."""
        TASK_OPERATIONS.labels(
            node_id=self.node_id,
            operation=operation,
            status='success' if success else 'failure',
        ).inc()
```

---

## 5. Add Health Endpoints (Priority: MEDIUM)

```python
# observability/health.py

from fastapi import FastAPI, Response
from pydantic import BaseModel
from typing import Optional
import asyncio

app = FastAPI(title="AetherGrid Node")

class HealthStatus(BaseModel):
    healthy: bool
    state: str
    term: int
    commit_index: int
    last_heartbeat_ago_ms: Optional[float]
    leader_id: Optional[int]

def create_health_app(node: 'RaftNodeV2') -> FastAPI:
    """Create FastAPI app with health endpoints."""
    
    @app.get("/health", response_model=HealthStatus)
    async def health():
        """Liveness probe."""
        now = asyncio.get_event_loop().time()
        last_heartbeat = node.last_heartbeat
        time_since_heartbeat = (now - last_heartbeat) * 1000  # ms
        
        # Consider unhealthy if:
        # - Follower hasn't received heartbeat in 3x election timeout
        # - Storage is corrupted
        is_healthy = True
        
        if node.state.value != 'leader':
            max_timeout = node.election_timeout_max * 3 * 1000
            if time_since_heartbeat > max_timeout:
                is_healthy = False
        
        return HealthStatus(
            healthy=is_healthy,
            state=node.state.value,
            term=node.current_term,
            commit_index=node.commit_index,
            last_heartbeat_ago_ms=time_since_heartbeat,
            leader_id=getattr(node, 'known_leader', None),
        )
    
    @app.get("/ready")
    async def ready():
        """Readiness probe."""
        # Node is ready if:
        # - It's the leader, OR
        # - It knows who the leader is
        
        if node.state.value == 'leader':
            return Response(status_code=200, content="Ready")
        
        if hasattr(node, 'known_leader') and node.known_leader is not None:
            return Response(status_code=200, content="Ready")
        
        return Response(status_code=503, content="Not Ready")
    
    @app.get("/metrics")
    async def metrics():
        """Prometheus metrics."""
        from prometheus_client import generate_latest
        return Response(
            content=generate_latest(),
            media_type="text/plain",
        )
    
    return app
```

---

## 6. Migration Path

### Step-by-Step Migration

```
Current State (v1.x)              Target State (v2.0)
─────────────────────────        ─────────────────────────
storage.py (JSON)        ──►     storage_v2.py (RocksDB)
node.py (in-memory)      ──►     node_v2.py (RocksDB backend)
simulator.py (queues)    ──►     grpc_transport.py
ast.literal_eval         ──►     SecureSerializer (JSON/Protobuf)
No metrics               ──►     Prometheus metrics
No health checks         ──►     FastAPI health endpoints
```

### Backward Compatibility

```python
# node_v2.py - Backward compatible with existing tests

class RaftNodeV2:
    """Updated Raft node with RocksDB storage."""
    
    def __init__(
        self,
        node_id: int,
        peers: Set[int],
        inbox: asyncio.Queue,  # Keep for backward compatibility
        send_message: Callable,
        storage: Optional[RaftStorageV2] = None,  # NEW: RocksDB storage
        **kwargs,
    ):
        # ... existing initialization ...
        
        # Use RocksDB if provided, else in-memory
        self.storage = storage
        
        if storage is None:
            # Fallback to in-memory for tests
            self._log: List[dict] = []
            self._snapshot_data: Optional[bytes] = None
```

---

## Running the Tests

```bash
# Run existing tests with new storage
python -m pytest tests/ -v

# Run with RocksDB storage
AETHERGRID_STORAGE=rocksdb python -m pytest tests/ -v

# Run benchmarks
python -m pytest tests/benchmark.py -v
```

---

## Next Steps

1. **Implement storage_v2.py** - RocksDB backend
2. **Fix security vulnerability** - Replace `ast.literal_eval`
3. **Add metrics** - Prometheus integration
4. **Add health endpoints** - FastAPI app
5. **Implement gRPC transport** - Replace simulator queues
6. **Update tests** - Ensure backward compatibility

See [ARCHITECTURE.md](ARCHITECTURE.md) for the complete blueprint.
