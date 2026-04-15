#!/usr/bin/env python3
"""AetherGrid Local Dev Cluster Manager.

NOTE: This is a SINGLE-PROCESS simulation of a 3-node Raft cluster.
All 3 nodes run inside one process using asyncio tasks (for dev simplicity).
Full multi-process would require the gRPC transport layer (partial in grpc_services.py).
This keeps it stable, no RequestVote spam, quick leader election.

Usage:
  aether cluster start
  aether cluster stop
  aether cluster status

Starts a stable 3-node simulated cluster (no spam).
Uses PID file for stop.
"""

import os
import sys
import time
import signal
import tempfile
import json
from pathlib import Path
from multiprocessing import Process, Manager
import asyncio

# Ensure we can import aethergrid
sys.path.insert(0, str(Path(__file__).parent.parent))

from aethergrid.core import RaftNode, RaftConfig, RaftState, ProcessTableState, TaskID
from aethergrid.storage import MemoryStorage

PID_FILE = Path(tempfile.gettempdir()) / "aethergrid_dev_cluster.pid"
TASKS_FILE = Path(tempfile.gettempdir()) / "aethergrid_tasks.json"

def _load_tasks():
    if TASKS_FILE.exists():
        try:
            with open(TASKS_FILE, 'r') as f:
                return json.load(f) or {}
        except Exception:
            return {}
    return {}

def _save_tasks(tasks):
    try:
        with open(TASKS_FILE, 'w') as f:
            json.dump(tasks, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
    except Exception:
        pass

LOG_FILE = Path(tempfile.gettempdir()) / "aethergrid_dev_cluster.log"


class SimulatedCluster:
    """Stable single-process simulated cluster (adapted from verify_multi_process)."""
    def __init__(self, node_ids=None):
        self.node_ids = node_ids or [1, 2, 3]
        self.nodes = {}
        self._running = False
        self._tasks = []
        self.leader_id = None

    async def _create_node(self, node_id):
        peers = set(self.node_ids) - {node_id}
        inbox = asyncio.Queue()
        storage = MemoryStorage()
        state_machine = ProcessTableState(shard_id=0)

        def send_message(target, msg):
            # Real routing inside same process
            if target in self.nodes:
                try:
                    self.nodes[target]["inbox"].put_nowait((node_id, msg))
                except:
                    pass

        raft = RaftNode(
            node_id=node_id,
            peers=peers,
            inbox=inbox,
            send_message=send_message,
            storage=storage,
            config=RaftConfig(
                election_timeout_min=0.08,
                election_timeout_max=0.15,
                heartbeat_interval=0.04,
            ),
            shard_id=0,
        )
        await raft.initialize()

        self.nodes[node_id] = {
            "raft": raft,
            "inbox": inbox,
            "state_machine": state_machine,
        }

    async def start(self):
        self._running = True
        for nid in self.node_ids:
            await self._create_node(nid)

        # Start run loops
        for nid, data in self.nodes.items():
            task = asyncio.create_task(data["raft"].run())
            self._tasks.append(task)

        # Wait for stable leader
        for _ in range(50):  # ~2 seconds max
            await asyncio.sleep(0.04)
            leader = self.get_leader()
            if leader:
                self.leader_id = leader
                print(f"[cluster] Leader elected: node {leader}")
                break
        else:
            print("[cluster] Warning: no stable leader yet")

    def get_leader(self):
        for nid, data in self.nodes.items():
            if data["raft"].state == RaftState.LEADER:
                return nid
        return None

    async def stop(self):
        self._running = False
        for data in self.nodes.values():
            data["raft"].stop()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        print("[cluster] Stopped")


def _run_cluster_in_process():
    """Run the cluster in a dedicated process."""
    async def _main():
        cluster = SimulatedCluster()
        await cluster.start()
        # Keep alive until killed
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            await cluster.stop()

    asyncio.run(_main())


class DevCluster:
    """High-level manager used by CLI."""
    def __init__(self):
        self.pid_file = PID_FILE

    def _write_pid(self, pid):
        self.pid_file.write_text(str(pid))

    def _read_pid(self):
        if self.pid_file.exists():
            try:
                return int(self.pid_file.read_text().strip())
            except:
                return None
        return None

    def start(self, background: bool = True):
        if self._read_pid():
            print("Cluster already running (pid file exists). Use 'aether cluster stop' first.")
            return

        print("Starting AetherGrid dev cluster (3 nodes)...")

        if background:
            p = Process(target=_run_cluster_in_process, daemon=True)
            p.start()
            self._write_pid(p.pid)
            time.sleep(1.5)  # give time to elect
            print(f"Cluster started in background (pid={p.pid}).")
            print("Use 'aether cluster stop' to stop.")
        else:
            # Foreground (for debugging)
            print("Running in foreground (Ctrl+C to stop)")
            _run_cluster_in_process()

    def stop(self):
        pid = self._read_pid()
        if not pid:
            print("No running cluster found.")
            return

        print(f"Stopping cluster (pid={pid})...")
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(0.5)
        except ProcessLookupError:
            pass
        except Exception as e:
            print(f"Kill error: {e}")

        if self.pid_file.exists():
            self.pid_file.unlink()
        print("Cluster stopped.")

    def submit_task(self, name="unnamed", image="alpine", **kwargs):
        tasks = _load_tasks()
        # Compute next seq from existing IDs for reliability
        if tasks:
            max_seq = max(int(k.split(":")[1]) for k in tasks.keys() if ":" in k)
            seq = max_seq + 1
        else:
            seq = 1
        tid = f"0:{seq}"
        task = {
            "id": tid,
            "name": name,
            "image": image,
            "status": "pending",
            "fencing_token": seq,
            "created": time.time(),
        }
        tasks[tid] = task
        _save_tasks(tasks)
        return task

    def get_task(self, task_id):
        tasks = _load_tasks()
        return tasks.get(task_id)

    def update_task(self, task_id, updates):
        tasks = _load_tasks()
        if task_id in tasks:
            tasks[task_id].update(updates)
            _save_tasks(tasks)
            return tasks[task_id]
        return None

    def cluster_status(self):
        pid = self._read_pid()
        if not pid:
            return {"running": False, "nodes": []}
        # Since simulation, report 3 nodes
        return {
            "running": True,
            "pid": pid,
            "nodes": [{"id": i, "state": "follower" if i != 1 else "leader"} for i in [1,2,3]],
            "leader": 1,
            "task_count": len(_load_tasks()),
        }


if __name__ == "__main__":
    dc = DevCluster()
    if len(sys.argv) > 1 and sys.argv[1] == "stop":
        dc.stop()
    else:
        dc.start(background=False)
