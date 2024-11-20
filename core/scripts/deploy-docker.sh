#!/usr/bin/env bash

# Start server.sh in the background
bash scripts/server.sh &

# Wait for server.sh to start by sleeping for a brief period (adjust as needed)
sleep 5

# Check if server.sh is still running; if not, exit with an error
if ! ps -p $! > /dev/null; then
    >&2 echo 'server.sh failed to start.'
    exit 1
fi

# Start workflow-compiling-service.sh in the background
bash scripts/workflow-compiling-service.sh &

# Wait for workflow-compiling-service.sh to start by sleeping for a brief period (adjust as needed)
sleep 5

# Check if workflow-compiling-service.sh is still running; if not, exit with an error
if ! ps -p $! > /dev/null; then
    >&2 echo 'workflow-compiling-service.sh failed to start.'
    exit 1
fi

# Start worker.sh in the background
bash scripts/worker.sh &

# Wait for one of server.sh and worker.sh to complete
wait -n
