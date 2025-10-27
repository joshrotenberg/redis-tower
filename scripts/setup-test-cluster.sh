#!/bin/bash
# Setup Redis Cluster for integration tests using local redis-server instances
#
# This script starts 6 Redis instances (3 masters + 3 replicas) on ports 7100-7105
# and initializes them as a cluster.
#
# Usage:
#   ./scripts/setup-test-cluster.sh start   # Start cluster
#   ./scripts/setup-test-cluster.sh stop    # Stop cluster
#   ./scripts/setup-test-cluster.sh status  # Check cluster status

set -e

PORTS=(7100 7101 7102 7103 7104 7105)
BASE_DIR="${TMPDIR:-/tmp}/redis-tower-test-cluster"

start_cluster() {
    echo "Starting Redis Cluster on ports ${PORTS[*]}..."

    # Create base directory
    mkdir -p "$BASE_DIR"

    # Start Redis instances
    for port in "${PORTS[@]}"; do
        mkdir -p "$BASE_DIR/$port"
        redis-server \
            --port "$port" \
            --cluster-enabled yes \
            --cluster-config-file "$BASE_DIR/$port/nodes.conf" \
            --cluster-node-timeout 5000 \
            --appendonly yes \
            --appendfilename "appendonly.aof" \
            --dbfilename "dump.rdb" \
            --dir "$BASE_DIR/$port" \
            --logfile "$BASE_DIR/$port/redis.log" \
            --daemonize yes \
            --pidfile "$BASE_DIR/$port/redis.pid"

        echo "  Started Redis on port $port"
    done

    # Wait for instances to start
    sleep 2

    # Create cluster
    echo "Creating cluster..."
    redis-cli --cluster create \
        127.0.0.1:7100 127.0.0.1:7101 127.0.0.1:7102 \
        127.0.0.1:7103 127.0.0.1:7104 127.0.0.1:7105 \
        --cluster-replicas 1 \
        --cluster-yes

    echo ""
    echo "Cluster started successfully!"
    echo "  Masters: 7100, 7101, 7102"
    echo "  Replicas: 7103, 7104, 7105"
    echo ""
    echo "Run tests with:"
    echo "  cargo test --test integration_cluster --features cluster -- --test-threads=1"
}

stop_cluster() {
    echo "Stopping Redis Cluster..."

    for port in "${PORTS[@]}"; do
        if [ -f "$BASE_DIR/$port/redis.pid" ]; then
            pid=$(cat "$BASE_DIR/$port/redis.pid")
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid"
                echo "  Stopped Redis on port $port (PID $pid)"
            fi
        else
            # Try killing by port if pidfile doesn't exist
            pkill -f "redis-server.*--port $port" 2>/dev/null || true
        fi
    done

    # Clean up data directory
    if [ -d "$BASE_DIR" ]; then
        rm -rf "$BASE_DIR"
        echo "  Cleaned up data directory: $BASE_DIR"
    fi

    echo "Cluster stopped."
}

status_cluster() {
    echo "Checking cluster status..."
    echo ""

    # Check if processes are running
    running=0
    for port in "${PORTS[@]}"; do
        if nc -z localhost "$port" 2>/dev/null; then
            echo "  Redis on port $port: RUNNING"
            ((running++))
        else
            echo "  Redis on port $port: NOT RUNNING"
        fi
    done

    echo ""

    if [ $running -eq 0 ]; then
        echo "Cluster is not running."
        exit 1
    fi

    # Check cluster info if any node is running
    if [ $running -gt 0 ]; then
        echo "Cluster info from port 7100:"
        redis-cli -p 7100 cluster info | grep -E "cluster_state|cluster_known_nodes|cluster_size"

        echo ""
        echo "Cluster nodes:"
        redis-cli -p 7100 cluster nodes
    fi
}

case "${1:-}" in
    start)
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    status)
        status_cluster
        ;;
    *)
        echo "Usage: $0 {start|stop|status}"
        echo ""
        echo "Commands:"
        echo "  start   - Start Redis Cluster on ports 7100-7105"
        echo "  stop    - Stop cluster and clean up data"
        echo "  status  - Check cluster status"
        exit 1
        ;;
esac
