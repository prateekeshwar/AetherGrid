#!/usr/bin/env python3
"""AetherGrid gRPC-based Cluster Manager.

Single-node cluster with real gRPC networking for task submission.

Usage:
  # Start cluster (leader + worker)
  python3 scripts/dev_cluster.py --port 50051
  
  # Or use CLI
  aether cluster start
  aether submit --name test --image python3 --args "script.py"
  aether status 0:1
  aether logs 0:1
"""

import os
import sys
import time
import signal
import tempfile
import json
import asyncio
import argparse
from pathlib import Path
from multiprocessing import Process

# Ensure we can import aethergrid
sys.path.insert(0, str(Path(__file__).parent.parent))

from aethergrid.core import RaftNode, RaftConfig, RaftState, ProcessTableState, TaskID, TaskStatus
from aethergrid.storage import MemoryStorage
from aethergrid.worker import WorkerAgent
from aethergrid.grpc_services import (
    create_grpc_server,
    create_task_client,
    submit_task_via_grpc,
    get_task_via_grpc,
    get_logs_via_grpc,
)

PID_FILE = Path(tempfile.gettempdir()) / "aethergrid_dev_cluster.pid"
TASKS_FILE = Path(tempfile.gettempdir()) / "aethergrid_tasks.json"
LOGS_FILE = Path(tempfile.gettempdir()) / "aethergrid_logs.json"
PORT_FILE = Path(tempfile.gettempdir()) / "aethergrid_port.txt"


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


def _load_logs():
    if LOGS_FILE.exists():
        try:
            with open(LOGS_FILE, 'r') as f:
                return json.load(f) or {}
        except Exception:
            return {}
    return {}


def _save_logs(logs):
    try:
        with open(LOGS_FILE, 'w') as f:
            json.dump(logs, f, indent=2)
    except Exception:
        pass


def _append_log(task_id: str, line: str):
    logs = _load_logs()
    if task_id not in logs:
        logs[task_id] = []
    logs[task_id].append({
        "time": time.time(),
        "line": line,
    })
    _save_logs(logs)


class GrpcCluster:
    """Single-node cluster with gRPC server and embedded worker."""
    
    def __init__(self, port: int = 50051):
        self.port = port
        self.node_id = 1
        self.peers = set()  # Single node for now
        self.raft_node = None
        self.worker = None
        self.grpc_server = None
        self._running = False
        self._tasks = []
    
    @property
    def state_machine(self):
        """Get the state machine from the Raft node."""
        return self.raft_node.state_machine if self.raft_node else None
    
    def _get_logs(self, task_id: str) -> list:
        """Get logs for a task (used by gRPC service)."""
        logs = _load_logs()
        return logs.get(task_id, [])
    
    async def _dispatch_task_to_worker(self, task):
        """Dispatch a submitted task to the embedded worker."""
        # Schedule the task
        schedule_result = self.state_machine.apply_command("schedule_task", {
            "task_id": task.id.to_dict(),
            "node_id": {"id": "worker-1"},
            "fencing_token": task.fencing_token,
            "lease_duration": 100000,
        })
        
        if not schedule_result.success:
            _append_log(f"{task.id.shard}:{task.id.sequence}", f"[ERROR] Failed to schedule: {schedule_result.error}")
            return
        
        # Create assignment for worker
        assignment = {
            "task_id": f"{task.id.shard}:{task.id.sequence}",
            "fencing_token": schedule_result.fencing_token,
            "lease_expiry_index": 100000,
            "image": task.image,
            "args": task.args,
            "env": task.env or [],
        }
        
        # Execute via worker
        _append_log(f"{task.id.shard}:{task.id.sequence}", f"[ASSIGNED] Task assigned to worker-1")
        asyncio.create_task(self.worker._execute_task(assignment))
    
    def _send_to_leader(self, msg: dict):
        """Handle messages from worker to leader."""
        msg_type = msg.get("type", "")
        
        if msg_type == "TaskStatusReport":
            task_id = msg.get("task_id", "")
            status = msg.get("status", "")
            exit_code = msg.get("exit_code", 0)
            error = msg.get("error_message", "")
            fencing_token = msg.get("fencing_token", 0)
            
            # Parse task_id
            parts = task_id.split(":")
            if len(parts) == 2:
                tid = TaskID(shard=int(parts[0]), sequence=int(parts[1]))
                
                # Update state machine based on status
                if status == "succeeded":
                    self.raft_node.state_machine.apply_command("complete_task", {
                        "task_id": tid.to_dict(),
                        "node_id": {"id": "worker-1"},
                        "fencing_token": fencing_token,
                        "exit_code": exit_code,
                    })
                elif status == "failed":
                    self.raft_node.state_machine.apply_command("fail_task", {
                        "task_id": tid.to_dict(),
                        "node_id": {"id": "worker-1"},
                        "fencing_token": fencing_token,
                        "error_message": error,
                    })
                elif status == "running":
                    self.raft_node.state_machine.apply_command("start_task", {
                        "task_id": tid.to_dict(),
                        "node_id": {"id": "worker-1"},
                        "fencing_token": fencing_token,
                    })
            
            # Update task file
            tasks = _load_tasks()
            if task_id in tasks:
                tasks[task_id]["status"] = status
                tasks[task_id]["exit_code"] = exit_code
                tasks[task_id]["error"] = error
                tasks[task_id]["updated"] = time.time()
                _save_tasks(tasks)
            
            _append_log(task_id, f"[STATUS] {status}" + (f" (exit={exit_code})" if exit_code else ""))
    
    async def start(self):
        """Start the cluster."""
        self._running = True
        
        # Create Raft node
        inbox = asyncio.Queue()
        storage = MemoryStorage()
        
        def send_message(target, msg):
            pass  # Single node, no peers
        
        self.raft_node = RaftNode(
            node_id=self.node_id,
            peers=self.peers,
            inbox=inbox,
            send_message=send_message,
            storage=storage,
            config=RaftConfig(
                election_timeout_min=0.05,
                election_timeout_max=0.10,
                heartbeat_interval=0.03,
            ),
            shard_id=0,
        )
        await self.raft_node.initialize()
        
        # Force become leader (single node) - don't start run loop
        # because it will reset our state
        self.raft_node.state = RaftState.LEADER
        self.raft_node._running = True
        
        # Create worker
        self.worker = WorkerAgent(
            node_id="worker-1",
            hostname="localhost",
            capacity={"cpu_millis": 4000, "memory_bytes": 8 * 1024 * 1024 * 1024},
            send_to_leader=self._send_to_leader,
            labels={"runtime": "process"},
            runtime="process",
        )
        
        # Create gRPC server
        self.grpc_server = create_grpc_server(
            raft_node=self.raft_node,
            state_machine=self.raft_node.state_machine,
            logs_store=self._get_logs,
            task_dispatcher=self._dispatch_task_to_worker,
            port=self.port,
        )
        
        # Start everything
        await self.grpc_server.start()
        
        # Start worker run loop
        worker_task = asyncio.create_task(self.worker.run())
        self._tasks.append(worker_task)
        
        # Save port for CLI
        PORT_FILE.write_text(str(self.port))
        
        print(f"[cluster] gRPC server started on port {self.port}")
        print(f"[cluster] Leader ready (node {self.node_id})")
        print(f"[cluster] Worker ready (worker-1)")
        print(f"[cluster] Commands: aether submit | aether status | aether logs")
    
    async def stop(self):
        """Stop the cluster."""
        self._running = False
        
        if self.worker:
            self.worker.stop()
        
        if self.grpc_server:
            await self.grpc_server.stop(grace=2)
        
        for t in self._tasks:
            t.cancel()
        
        print("[cluster] Stopped")
    
    async def run_forever(self):
        """Run until interrupted."""
        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()


def _run_cluster_in_process(port: int = 50051):
    """Run the cluster in a dedicated process."""
    async def _main():
        cluster = GrpcCluster(port=port)
        await cluster.start()
        await cluster.run_forever()
    
    asyncio.run(_main())


class DevCluster:
    """High-level manager used by CLI."""
    
    def __init__(self):
        self.pid_file = PID_FILE
        self._port = None
    
    def _write_pid(self, pid):
        self.pid_file.write_text(str(pid))
    
    def _read_pid(self):
        if self.pid_file.exists():
            try:
                return int(self.pid_file.read_text().strip())
            except:
                return None
        return None
    
    def _read_port(self):
        if PORT_FILE.exists():
            try:
                return int(PORT_FILE.read_text().strip())
            except:
                return 50051
        return 50051
    
    def start(self, port: int = 50051, background: bool = True):
        if self._read_pid():
            print("Cluster already running. Use 'aether cluster stop' first.")
            return
        
        print(f"Starting AetherGrid cluster on port {port}...")
        
        if background:
            p = Process(target=_run_cluster_in_process, args=(port,), daemon=True)
            p.start()
            self._write_pid(p.pid)
            time.sleep(0.5)
            print(f"Cluster started (pid={p.pid}, port={port})")
            print("Commands: aether submit | aether status | aether logs")
        else:
            print("Running in foreground (Ctrl+C to stop)")
            _run_cluster_in_process(port)
    
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
        if PORT_FILE.exists():
            PORT_FILE.unlink()
        print("Cluster stopped.")
    
    def submit_task(self, name="unnamed", image="python3", args=None, **kwargs):
        """Submit a task via gRPC."""
        port = self._read_port()
        
        # Run async submission
        async def _submit():
            client = create_task_client(f"localhost:{port}")
            result = await submit_task_via_grpc(
                client=client,
                name=name,
                image=image,
                args=args or [],
            )
            return result
        
        result = asyncio.run(_submit())
        
        if result.get("success"):
            # Save to tasks file for CLI queries
            tasks = _load_tasks()
            task_id = result["task_id"]
            tasks[task_id] = {
                "id": task_id,
                "name": name,
                "image": image,
                "args": args or [],
                "status": "pending",
                "created": time.time(),
            }
            _save_tasks(tasks)
            _append_log(task_id, f"[SUBMIT] Task submitted: {name}")
            return tasks[task_id]
        else:
            raise Exception(result.get("error", "Failed to submit task"))
    
    def get_task(self, task_id: str):
        """Get task status via gRPC."""
        port = self._read_port()
        
        async def _get():
            client = create_task_client(f"localhost:{port}")
            return await get_task_via_grpc(client, task_id)
        
        result = asyncio.run(_get())
        
        if result.get("found"):
            # Update local cache
            tasks = _load_tasks()
            tasks[task_id] = {
                "id": task_id,
                "name": result.get("name", ""),
                "status": result.get("status", "").replace("TASK_STATUS_", "").lower(),
                "exit_code": result.get("exit_code", 0),
                "error": result.get("error_message", ""),
                "image": result.get("image", ""),
                "args": result.get("args", []),
            }
            _save_tasks(tasks)
            return tasks[task_id]
        
        return None
    
    def get_logs(self, task_id: str) -> list:
        """Get task logs via gRPC."""
        port = self._read_port()
        
        async def _get():
            client = create_task_client(f"localhost:{port}")
            return await get_logs_via_grpc(client, task_id)
        
        result = asyncio.run(_get())
        return [{"line": line} for line in result.get("lines", [])]
    
    def update_task(self, task_id: str, updates: dict):
        """Update task (local cache only)."""
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
        return {
            "running": True,
            "pid": pid,
            "port": self._read_port(),
            "nodes": [{"id": 1, "state": "leader"}],
            "leader": 1,
            "task_count": len(_load_tasks()),
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AetherGrid gRPC Cluster")
    parser.add_argument("--port", type=int, default=50051, help="Port to listen on")
    parser.add_argument("--stop", action="store_true", help="Stop running cluster")
    args = parser.parse_args()
    
    dc = DevCluster()
    
    if args.stop:
        dc.stop()
    else:
        dc.start(port=args.port, background=False)
