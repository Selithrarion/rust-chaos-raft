#!/bin/bash

start_and_monitor_node() {
    ID=$1
    while true; do
        cargo run -p raft_node -- --id $ID
        echo "[OBSERVER] Node $ID exited. Restarting in 1 second..."
        sleep 1
    done
}

start_and_monitor_node 1 &
start_and_monitor_node 2 &
start_and_monitor_node 3 &

echo "Cluster nodes started with auto-restart."
echo "Press Ctrl+C to stop"

cleanup() {
    echo "Stopping cluster nodes..."
    pkill -P $$
}

trap cleanup INT
wait