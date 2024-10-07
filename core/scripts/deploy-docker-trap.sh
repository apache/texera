#!/usr/bin/env bash

# Start server.sh in the background
bash scripts/server.sh &
SERVER_PID=$!  # Store the PID of server.sh

# Wait for server.sh to start by sleeping for a brief period (adjust as needed)
sleep 5

# Check if server.sh is still running; if not, exit with an error
if ! ps -p $SERVER_PID > /dev/null; then
    >&2 echo 'server.sh failed to start.'
    exit 1
fi

# Start worker.sh in the background
bash scripts/worker.sh &
WORKER_PID=$!  # Store the PID of worker.sh

# Trap SIGTERM and SIGINT and forward them as SIGTERM to the background processes
trap 'kill -TERM $SERVER_PID; kill -TERM $WORKER_PID; wait $SERVER_PID $WORKER_PID' SIGTERM SIGINT

# Wait for one of server.sh or worker.sh to complete
wait -n

# # Send SIGTERM to both processes in case one exits early
# kill -TERM $SERVER_PID $WORKER_PID

# # Give them time to shut down gracefully
# wait $SERVER_PID $WORKER_PID