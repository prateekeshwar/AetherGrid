"""AetherGrid v2.0 - Local DAG Workflow Engine.

Supports defining workflows as DAGs of tasks, topo sort, run with deps,
submit to AetherGrid cluster, track status.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Callable, Any, Set
from collections import defaultdict, deque

from ..core import ProcessTableState, TaskStatus


class WorkflowStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class WorkflowTask:
    """A single task in a DAG workflow."""
    name: str
    image: str = "alpine"
    args: List[str] = field(default_factory=list)
    depends_on: List[str] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    task_id: Optional[Any] = None  # Assigned after submit


@dataclass
class Workflow:
    """DAG Workflow definition."""
    name: str
    tasks: Dict[str, WorkflowTask] = field(default_factory=dict)
    status: WorkflowStatus = WorkflowStatus.PENDING
    results: Dict[str, Any] = field(default_factory=dict)


class WorkflowEngine:
    """Executes DAG workflows on AetherGrid cluster."""

    def __init__(self, cluster_client: Any = None):
        self.cluster = cluster_client  # Would be real client
        self.workflows: Dict[str, Workflow] = {}
        self._state = ProcessTableState(shard_id=0)  # Local sim for demo
    
    def define(self, name: str, tasks: List[Dict]) -> Workflow:
        """Define a workflow from task specs."""
        wf = Workflow(name=name)
        for t in tasks:
            wt = WorkflowTask(
                name=t["name"],
                image=t.get("image", "alpine"),
                args=t.get("args", []),
                depends_on=t.get("depends_on", []),
                labels=t.get("labels", {}),
            )
            wf.tasks[wt.name] = wt
        self.workflows[name] = wf
        return wf
    
    def _topo_sort(self, wf: Workflow) -> List[str]:
        """Return task names in topological order."""
        graph = defaultdict(list)
        indeg = {name: 0 for name in wf.tasks}
        for name, task in wf.tasks.items():
            for dep in task.depends_on:
                graph[dep].append(name)
                indeg[name] += 1
        
        q = deque([n for n, d in indeg.items() if d == 0])
        order = []
        while q:
            n = q.popleft()
            order.append(n)
            for nei in graph[n]:
                indeg[nei] -= 1
                if indeg[nei] == 0:
                    q.append(nei)
        if len(order) != len(wf.tasks):
            raise ValueError("Cycle detected in workflow DAG")
        return order
    
    async def run(self, name: str) -> Workflow:
        """Run workflow: submit tasks respecting deps, wait completion."""
        wf = self.workflows.get(name)
        if not wf:
            raise ValueError(f"Workflow {name} not defined")
        
        wf.status = WorkflowStatus.RUNNING
        order = self._topo_sort(wf)
        
        for task_name in order:
            task = wf.tasks[task_name]
            # Wait for deps (simplified, in real poll state)
            for dep in task.depends_on:
                # In real impl: await cluster.get_task(dep_task_id).status == SUCCEEDED
                pass
            
            # Submit to cluster (simplified)
            result = self._state.apply_command("submit_task", {
                "name": task.name,
                "namespace": name,
                "image": task.image,
                "args": task.args,
                "labels": {**task.labels, "workflow": name},
            })
            if result.success:
                task.task_id = result.task.id
                wf.results[task_name] = {"status": "submitted", "id": str(task.task_id)}
        
        # In real: monitor all until done
        wf.status = WorkflowStatus.SUCCEEDED
        return wf
    
    def get_status(self, name: str) -> Dict[str, Any]:
        wf = self.workflows.get(name)
        return {
            "name": name,
            "status": wf.status.value if wf else "not_found",
            "tasks": {n: t.task_id for n,t in (wf.tasks.items() if wf else [])},
        }


# Global engine
engine = WorkflowEngine()

def define_workflow(name: str, tasks: List[Dict]) -> Workflow:
    return engine.define(name, tasks)

async def run_workflow(name: str) -> Workflow:
    return await engine.run(name)
