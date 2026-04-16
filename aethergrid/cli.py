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
    
    worker_app = typer.Typer(help="Worker management")
    app.add_typer(worker_app, name="worker")
    
    workflow_app = typer.Typer(help="DAG workflow commands")
    app.add_typer(workflow_app, name="workflow")

    # --- Submit ---
    @app.command()
    def submit(
        name: str = typer.Option("unnamed", "--name", "-n", help="Task name"),
        image: str = typer.Option("python3", "--image", "-i", help="Docker image or executable (e.g., python3, ubuntu:latest)"),
        args: str = typer.Option("", "--args", "-a", help="Comma-separated args"),
        gpu: int = typer.Option(0, "--gpu", "-g", help="Number of GPUs required"),
        cpu: int = typer.Option(1000, "--cpu", "-c", help="CPU in milli-cores (default: 1000 = 1 CPU)"),
        memory: int = typer.Option(1024, "--memory", "-m", help="Memory in MB (default: 1024MB)"),
        gpu_memory: int = typer.Option(0, "--gpu-memory", help="GPU memory in MB"),
        env: str = typer.Option("", "--env", "-e", help="Comma-separated env vars (KEY=VALUE)"),
        namespace: str = typer.Option("default", "--namespace", help="Task namespace"),
        priority: int = typer.Option(0, "--priority", "-p", help="Task priority (higher = more important)"),
    ):
        """Submit a task via gRPC with resource requirements."""
        try:
            # Parse comma-separated args into list
            args_list = [a.strip() for a in args.split(",")] if args else []
            
            # Parse environment variables
            env_list = [e.strip() for e in env.split(",")] if env else []
            
            # Build resource requirements
            resources = {
                "cpu_millis": cpu,
                "memory_bytes": memory * 1024 * 1024,  # Convert MB to bytes
                "gpu_count": gpu,
                "gpu_memory_mb": gpu_memory,
            }
            
            task = _cluster.submit_task(
                name=name, 
                image=image, 
                args=args_list,
                env=env_list,
                resources=resources,
                namespace=namespace,
                priority=priority,
            )
            typer.echo(f"Submitted task {task['id']}")
            typer.echo(f"  Name: {name}")
            typer.echo(f"  Image: {image}")
            typer.echo(f"  Args: {args}")
            typer.echo(f"  Priority: {priority}")
            typer.echo(f"  Resources: CPU={cpu}m, Memory={memory}MB, GPU={gpu}")
            if gpu > 0:
                typer.echo(f"  GPU Memory: {gpu_memory}MB")
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
    
    @cluster_app.command("resources")
    def cluster_resources():
        """Show cluster resource summary (CPU, Memory, GPU)."""
        resources = _cluster.get_cluster_resources()
        if not resources:
            typer.echo("Cluster not running. Use 'aether cluster start'.")
            raise typer.Exit(1)
        
        typer.echo("Cluster Resources:")
        typer.echo("-" * 50)
        cap = resources.get("capacity", {})
        avail = resources.get("available", {})
        alloc = resources.get("allocated", {})
        
        typer.echo(f"  Nodes: {resources.get('node_count', 0)}")
        typer.echo(f"  GPU Nodes: {resources.get('gpu_nodes', 0)}")
        typer.echo()
        typer.echo("  CPU:")
        typer.echo(f"    Capacity:  {cap.get('cpu_millis', 0)}m")
        typer.echo(f"    Available: {avail.get('cpu_millis', 0)}m")
        typer.echo(f"    Allocated: {alloc.get('cpu_millis', 0)}m")
        typer.echo()
        typer.echo("  Memory:")
        typer.echo(f"    Capacity:  {cap.get('memory_bytes', 0) // (1024*1024)}MB")
        typer.echo(f"    Available: {avail.get('memory_bytes', 0) // (1024*1024)}MB")
        typer.echo(f"    Allocated: {alloc.get('memory_bytes', 0) // (1024*1024)}MB")
        typer.echo()
        typer.echo("  GPU:")
        typer.echo(f"    Capacity:  {cap.get('gpu_count', 0)} GPUs, {cap.get('gpu_memory_mb', 0)}MB VRAM")
        typer.echo(f"    Available: {avail.get('gpu_count', 0)} GPUs, {avail.get('gpu_memory_mb', 0)}MB VRAM")
        typer.echo(f"    Allocated: {alloc.get('gpu_count', 0)} GPUs, {alloc.get('gpu_memory_mb', 0)}MB VRAM")
    
    @cluster_app.command("top")
    def cluster_top():
        """Show cluster dashboard with node details and GPU utilization."""
        nodes = _cluster.get_cluster_nodes()
        if not nodes:
            typer.echo("Cluster not running. Use 'aether cluster start'.")
            raise typer.Exit(1)
        
        # Header
        typer.echo("\n" + "=" * 90)
        typer.echo("  AETHERGRID CLUSTER DASHBOARD")
        typer.echo("=" * 90)
        
        # Summary row
        total_gpus = sum(n.get("capacity", {}).get("gpu_count", 0) for n in nodes)
        avail_gpus = sum(n.get("available", {}).get("gpu_count", 0) for n in nodes)
        total_vram = sum(n.get("capacity", {}).get("gpu_memory_mb", 0) for n in nodes)
        avail_vram = sum(n.get("available", {}).get("gpu_memory_mb", 0) for n in nodes)
        total_cpu = sum(n.get("capacity", {}).get("cpu_millis", 0) for n in nodes)
        avail_cpu = sum(n.get("available", {}).get("cpu_millis", 0) for n in nodes)
        total_mem = sum(n.get("capacity", {}).get("memory_bytes", 0) for n in nodes) // (1024*1024)
        avail_mem = sum(n.get("available", {}).get("memory_bytes", 0) for n in nodes) // (1024*1024)
        running_tasks = sum(n.get("running_task_count", 0) for n in nodes)
        
        typer.echo(f"\n  Cluster Summary: {len(nodes)} node(s), {running_tasks} running task(s)")
        typer.echo(f"  CPU: {avail_cpu}m / {total_cpu}m available  |  Memory: {avail_mem}MB / {total_mem}MB available")
        typer.echo(f"  GPU: {avail_gpus} / {total_gpus} available  |  VRAM: {avail_vram}MB / {total_vram}MB available")
        
        # Node table
        typer.echo("\n" + "-" * 90)
        typer.echo(f"  {'Node ID':<20} {'Status':<10} {'CPU (m)':<12} {'Memory':<12} {'GPU':<8} {'VRAM':<12} {'Tasks':<6}")
        typer.echo("-" * 90)
        
        for node in nodes:
            node_id = node.get("id", "unknown")[:18]
            status = node.get("health", "unknown")
            cap = node.get("capacity", {})
            avail = node.get("available", {})
            tasks = node.get("running_task_count", 0)
            
            # CPU
            cpu_cap = cap.get("cpu_millis", 0)
            cpu_avail = avail.get("cpu_millis", 0)
            cpu_used = cpu_cap - cpu_avail
            cpu_str = f"{cpu_used}/{cpu_cap}"
            
            # Memory
            mem_cap = cap.get("memory_bytes", 0) // (1024*1024)
            mem_avail = avail.get("memory_bytes", 0) // (1024*1024)
            mem_used = mem_cap - mem_avail
            mem_str = f"{mem_used}/{mem_cap}MB"
            
            # GPU
            gpu_cap = cap.get("gpu_count", 0)
            gpu_avail = avail.get("gpu_count", 0)
            gpu_used = gpu_cap - gpu_avail
            gpu_str = f"{gpu_used}/{gpu_cap}" if gpu_cap > 0 else "-"
            
            # VRAM
            vram_cap = cap.get("gpu_memory_mb", 0)
            vram_avail = avail.get("gpu_memory_mb", 0)
            vram_used = vram_cap - vram_avail
            vram_str = f"{vram_used}/{vram_cap}MB" if vram_cap > 0 else "-"
            
            # Status color indicator
            status_icon = "🟢" if status == "healthy" else "🟡" if status == "degraded" else "🔴"
            
            typer.echo(f"  {node_id:<20} {status_icon} {status:<8} {cpu_str:<12} {mem_str:<12} {gpu_str:<8} {vram_str:<12} {tasks:<6}")
        
        typer.echo("-" * 90)
        typer.echo("")

    # --- Worker ---
    @worker_app.command("start")
    def worker_start(
        node_id: str = typer.Option(..., "--node-id", "-n", help="Unique node ID"),
        cluster: str = typer.Option("localhost:50051", "--cluster", "-c", help="Cluster address (host:port)"),
        hostname: str = typer.Option(None, "--hostname", help="Hostname (auto-detected if not set)"),
        cpu: int = typer.Option(4000, "--cpu", help="CPU capacity in milli-cores"),
        memory: int = typer.Option(8192, "--memory", "-m", help="Memory capacity in MB"),
        gpu_count: int = typer.Option(0, "--gpu-count", "-g", help="Number of GPUs"),
        gpu_memory: int = typer.Option(0, "--gpu-memory", help="GPU memory in MB"),
        runtime: str = typer.Option("docker", "--runtime", "-r", help="Runtime type (docker/process)"),
        labels: str = typer.Option("", "--labels", "-l", help="Comma-separated labels (key=value)"),
        background: bool = typer.Option(False, "--background/--foreground", help="Run in background"),
    ):
        """Start a standalone worker that joins the cluster."""
        from .worker.daemon import WorkerDaemon
        from multiprocessing import Process
        
        typer.echo(f"Starting worker {node_id}...")
        typer.echo(f"  Cluster: {cluster}")
        typer.echo(f"  CPU: {cpu}m, Memory: {memory}MB")
        if gpu_count > 0:
            typer.echo(f"  GPUs: {gpu_count}, GPU Memory: {gpu_memory}MB")
        
        # Parse labels
        labels_dict = {}
        if labels:
            for label in labels.split(","):
                if "=" in label:
                    k, v = label.split("=", 1)
                    labels_dict[k.strip()] = v.strip()
        
        if background:
            # Run in background process
            def run_in_process():
                import asyncio
                daemon = WorkerDaemon(
                    node_id=node_id,
                    cluster_address=cluster,
                    hostname=hostname,
                    cpu_millis=cpu,
                    memory_mb=memory,
                    gpu_count=gpu_count,
                    gpu_memory_mb=gpu_memory,
                    labels=labels_dict,
                    runtime=runtime,
                )
                asyncio.run(daemon.run())
            
            p = Process(target=run_in_process, daemon=True)
            p.start()
            typer.echo(f"Worker started (pid={p.pid})")
        else:
            # Run in foreground
            import asyncio
            daemon = WorkerDaemon(
                node_id=node_id,
                cluster_address=cluster,
                hostname=hostname,
                cpu_millis=cpu,
                memory_mb=memory,
                gpu_count=gpu_count,
                gpu_memory_mb=gpu_memory,
                labels=labels_dict,
                runtime=runtime,
            )
            try:
                asyncio.run(daemon.run())
            except KeyboardInterrupt:
                daemon.stop()
                typer.echo("\nWorker stopped")

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
