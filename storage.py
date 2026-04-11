"""AetherGrid Core - Persistent Storage (WAL).

Provides durable storage for Raft node state with fsync guarantees.
"""

import asyncio
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class RaftStorage:
    """Persistent storage for a single Raft node.

    Stores:
    - current_term
    - voted_for
    - log entries (WAL)
    - snapshot (last_included_index, last_included_term, state_machine)
    """

    node_id: int
    data_dir: str = "data"

    def __post_init__(self) -> None:
        """Ensure data directory exists."""
        os.makedirs(self.data_dir, exist_ok=True)
        self.term_file = os.path.join(self.data_dir, f"node_{self.node_id}_term.json")
        self.vote_file = os.path.join(self.data_dir, f"node_{self.node_id}_vote.json")
        self.log_file = os.path.join(self.data_dir, f"node_{self.node_id}_log.json")
        self.snapshot_file = os.path.join(self.data_dir, f"node_{self.node_id}_snapshot.json")

    async def _write_json_async(self, path: str, data: Any) -> None:
        """Write JSON to file asynchronously with fsync."""
        loop = asyncio.get_event_loop()

        def _sync_write() -> None:
            # Ensure directory exists
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            # Write to temp file then rename for atomicity
            tmp_path = path + ".tmp"
            with open(tmp_path, "w") as f:
                json.dump(data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, path)
            # Fsync the directory to ensure rename is durable
            dir_fd = os.open(os.path.dirname(path) or ".", os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

        await loop.run_in_executor(None, _sync_write)

    async def save_term(self, term: int) -> None:
        """Persist current_term with fsync."""
        await self._write_json_async(self.term_file, {"current_term": term})

    async def save_vote(self, voted_for: Optional[int]) -> None:
        """Persist voted_for with fsync."""
        await self._write_json_async(self.vote_file, {"voted_for": voted_for})

    async def save_log_entry(self, index: int, entry: dict) -> None:
        """Append a single log entry to WAL with fsync."""
        # Read existing log, append, write back (simple append would be better with a real WAL)
        loop = asyncio.get_event_loop()

        def _sync_append() -> None:
            # Load existing log
            log_data = []
            if os.path.exists(self.log_file):
                with open(self.log_file, "r") as f:
                    try:
                        log_data = json.load(f).get("entries", [])
                    except json.JSONDecodeError:
                        log_data = []
            # Ensure we have the right index (append or overwrite)
            while len(log_data) < index:
                log_data.append(None)
            log_data[index - 1] = entry
            # Filter out None gaps
            log_data = [e for e in log_data if e is not None]
            with open(self.log_file, "w") as f:
                json.dump({"entries": log_data}, f, indent=2)
                f.flush()
                os.fsync(f.fileno())

        await loop.run_in_executor(None, _sync_append)

    async def save_log(self, entries: List[dict]) -> None:
        """Persist entire log (used for compaction) with fsync."""
        await self._write_json_async(self.log_file, {"entries": entries})

    async def save_snapshot(
        self,
        last_included_index: int,
        last_included_term: int,
        state_machine: dict,
    ) -> None:
        """Persist snapshot with fsync. Also clears the log file."""
        await self._write_json_async(
            self.snapshot_file,
            {
                "last_included_index": last_included_index,
                "last_included_term": last_included_term,
                "state_machine": state_machine,
            },
        )
        # Clear log file (entries before snapshot are now in snapshot)
        await self.save_log([])

    async def load_all(self) -> Dict[str, Any]:
        """Load all persisted state. Returns dict with keys: term, voted_for, log, snapshot."""
        loop = asyncio.get_event_loop()

        def _sync_load() -> Dict[str, Any]:
            result: Dict[str, Any] = {
                "term": 0,
                "voted_for": None,
                "log": [],
                "snapshot": {
                    "last_included_index": 0,
                    "last_included_term": 0,
                    "state_machine": {},
                },
            }

            # Load term
            if os.path.exists(self.term_file):
                with open(self.term_file, "r") as f:
                    try:
                        data = json.load(f)
                        result["term"] = data.get("current_term", 0)
                    except json.JSONDecodeError:
                        pass

            # Load vote
            if os.path.exists(self.vote_file):
                with open(self.vote_file, "r") as f:
                    try:
                        data = json.load(f)
                        result["voted_for"] = data.get("voted_for")
                    except json.JSONDecodeError:
                        pass

            # Load log
            if os.path.exists(self.log_file):
                with open(self.log_file, "r") as f:
                    try:
                        data = json.load(f)
                        result["log"] = data.get("entries", [])
                    except json.JSONDecodeError:
                        pass

            # Load snapshot
            if os.path.exists(self.snapshot_file):
                with open(self.snapshot_file, "r") as f:
                    try:
                        data = json.load(f)
                        result["snapshot"]["last_included_index"] = data.get("last_included_index", 0)
                        result["snapshot"]["last_included_term"] = data.get("last_included_term", 0)
                        result["snapshot"]["state_machine"] = data.get("state_machine", {})
                    except json.JSONDecodeError:
                        pass

            return result

        return await loop.run_in_executor(None, _sync_load)
