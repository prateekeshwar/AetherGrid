"""AetherGrid CLI (Typer-based).

Commands:
  aether submit --name myjob --image python3 --args "script.py,arg1"
  aether status 0:1
  aether logs 0:1
  aether cluster start / stop / status
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Optional

try:
    import typer
except ImportError:
    typer = None

# Add project root for local imports when run as script
sys.path.insert(0, str(Path(__file__).parent.parent))

from .workflow import define_workflow, run_workflow
from .testing import benchmark_task_submission
from scripts.dev_cluster import DevCluster

# Use shared cluster state (file-backed for cross-invocation)
_cluster = DevCluster()


def main():
    """Entry point for 'aether' command."""
    if typer is None:
        print("ERROR: 'typer' is required for the aether CLI.")
        print("Install with: pip install -e '.[dev]'  (or pip install typer)")
        print("Then activate your venv and re-run.")
        sys.exit(1)
    
    # Build Typer app here (after confirming typer exists)
    app = typer.Typer(name="aether", help="AetherGrid CLI - Distributed Task Orchestration via gRPC")
    
    cluster_app = typer.Typer(help="Cluster management")
    app.add_typer(cluster_app, name="cluster")
    
    workflow_app = typer.Typer(help="DAG workflow commands")
    app.add_typer(workflow_app, name="workflow")

    # --- Submit ---
    @app.command()
    def submit(
        name: str = typer.Option("unnamed", "--name", "-n", help="Task name"),
        image: str = typer.Option("python3", "--image", "-i", help="Executable (python3, /path/to/script)"),
        args: str = typer.Option("", "--args", "-a", help="Comma-separated args"),
    ):
        """Submit a task via gRPC."""
        try:
            # Parse comma-separated args into list
            args_list = [a.strip() for a in args.split(",")] if args else []
            task = _cluster.submit_task(name=name, image=image, args=args_list)
            typer.echo(f"Submitted task {task['id']}")
            typer.echo(f"  Name: {name}")
            typer.echo(f"  Image: {image}")
            typer.echo(f"  Args: {args}")
            typer.echo(f"\nCheck status: aether status {task['id']}")
        except Exception as e:
            typer.echo(f"Error: {e}", err=True)
            typer.echo("Is the cluster running? Use 'aether cluster start'", err=True)
            raise typer.Exit(1)

    # --- Status ---
    @app.command()
    def status(task_id: str = typer.Argument(..., help="Task ID e.g. 0:1")):
        """Get task status via gRPC."""
        task = _cluster.get_task(task_id)
        if task:
            typer.echo(json.dumps(task, indent=2))
        else:
            typer.echo("Task not found", err=True)
            raise typer.Exit(1)

    # --- Logs ---
    @app.command()
    def logs(task_id: str = typer.Argument(..., help="Task ID e.g. 0:1")):
        """Show task logs via gRPC."""
        task = _cluster.get_task(task_id)
        if not task:
            typer.echo("Task not found", err=True)
            raise typer.Exit(1)
        
        task_logs = _cluster.get_logs(task_id)
        typer.echo(f"Logs for task {task_id} (status={task.get('status')}):")
        typer.echo("-" * 40)
        for entry in task_logs:
            typer.echo(entry.get("line", ""))

    # --- Cancel ---
    @app.command()
    def cancel(task_id: str = typer.Argument(..., help="Task ID e.g. 0:1")):
        """Cancel a running/pending task."""
        task = _cluster.update_task(task_id, {"status": "cancelled"})
        if task:
            typer.echo(f"Cancelled task {task_id}")
        else:
            typer.echo("Task not found", err=True)

    # --- Cluster ---
    @cluster_app.command("start")
    def cluster_start(
        port: int = typer.Option(50051, "--port", "-p", help="gRPC port"),
        background: bool = typer.Option(True, "--background/--no-background"),
    ):
        """Start gRPC cluster (leader + worker)."""
        cluster = DevCluster()
        cluster.start(port=port, background=background)

    @cluster_app.command("stop")
    def cluster_stop():
        """Stop gRPC cluster."""
        cluster = DevCluster()
        cluster.stop()

    @cluster_app.command("status")
    def cluster_status():
        """Show cluster status."""
        status = _cluster.cluster_status()
        if not status["running"]:
            typer.echo("Cluster not running. Use 'aether cluster start'.")
        else:
            typer.echo(json.dumps(status, indent=2))

    # --- Workflow ---
    @workflow_app.command("run")
    def workflow_run(file: Path = typer.Argument(..., help="Workflow YAML file")):
        """Run a DAG workflow."""
        if not file.exists():
            typer.echo(f"File not found: {file}")
            if typer.confirm("Create a sample workflow template?", default=True):
                file.write_text("""name: sample
tasks:
  - name: step1
    image: python3
    args: ["-c", "print('hello')"]
  - name: step2
    image: python3
    args: ["-c", "print('world')"]
    depends_on: [step1]
""")
            typer.echo("Template created. Re-run the command.")
            raise typer.Exit(0)
        try:
            import yaml
            spec = yaml.safe_load(file.read_text())
        except Exception:
            spec = {"name": file.stem, "tasks": []}
        wf_name = spec.get("name", "cli-workflow")
        define_workflow(wf_name, spec.get("tasks", []))
        typer.echo(f"Defined workflow: {wf_name}")
        result = asyncio.run(run_workflow(wf_name))
        typer.echo(f"Status: {result.status.value}")

    # --- Benchmark ---
    @app.command()
    def benchmark(n: int = typer.Argument(100)):
        """Run quick benchmark."""
        res = asyncio.run(benchmark_task_submission(n))
        typer.echo(json.dumps(res, indent=2))

    # Run the app
    app()


if __name__ == "__main__":
    main()
