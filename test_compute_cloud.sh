#!/bin/bash
# AetherGrid Compute Cloud Test Script
# 
# This script demonstrates the complete AI compute cloud functionality:
# 1. Starts the cluster leader with embedded worker
# 2. Waits for the cluster to be ready
# 3. Submits a test task
# 4. Shows the cluster dashboard
#
# Usage: ./test_compute_cloud.sh

set -e

echo "=========================================="
echo "  AetherGrid Compute Cloud Test"
echo "=========================================="
echo ""

# Cleanup any existing cluster
echo "[1/5] Cleaning up any existing cluster..."
pkill -f "dev_cluster.py" 2>/dev/null || true
rm -f /tmp/aethergrid_*.pid /tmp/aethergrid_*.txt /tmp/aethergrid_*.json 2>/dev/null || true
sleep 2

# Start the cluster using Python directly (bypasses PID file check)
echo "[2/5] Starting AetherGrid cluster..."
python3 -c "
import asyncio
import sys
sys.path.insert(0, '.')
from scripts.dev_cluster import GrpcCluster

async def main():
    cluster = GrpcCluster(port=50051)
    await cluster.start()
    await cluster.run_forever()

asyncio.run(main())
" &
CLUSTER_PID=$!
echo "      Cluster PID: $CLUSTER_PID"

# Write PID file for CLI commands
echo $CLUSTER_PID > /tmp/aethergrid_dev_cluster.pid
echo "50051" > /tmp/aethergrid_port.txt

# Wait for cluster to be ready
echo "[3/5] Waiting for cluster to be ready..."
sleep 3

# Verify cluster is responding
for i in {1..10}; do
    if python3 -c "
import grpc
channel = grpc.insecure_channel('127.0.0.1:50051')
grpc.channel_ready_future(channel).result(timeout=1)
channel.close()
" 2>/dev/null; then
        echo "      Cluster is ready!"
        break
    fi
    echo "      Waiting... ($i/10)"
    sleep 1
done

# Submit a test task
echo "[4/5] Submitting test task..."
aether submit --name hello-ai --image python3 --args "-c,print('Hello from AetherGrid AI Compute Cloud!')" --priority 10

# Wait for task to complete
sleep 2

# Show cluster dashboard
echo "[5/5] Showing cluster dashboard..."
aether cluster top

# Show task status
echo ""
echo "Task Status:"
aether status 0:0

# Show task logs
echo ""
echo "Task Logs:"
aether logs 0:0

# Cleanup
echo ""
echo "=========================================="
echo "  Test Complete!"
echo "=========================================="
echo ""
echo "The cluster is still running. To stop it:"
echo "  kill $CLUSTER_PID"
echo "  rm -f /tmp/aethergrid_*.pid /tmp/aethergrid_*.txt /tmp/aethergrid_*.json"
