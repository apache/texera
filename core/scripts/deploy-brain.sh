#!/usr/bin/env bash

# Start brain.sh in the background
bash scripts/brain.sh &
BRAIN_PID=$!  # Store the PID of server.sh

# Trap SIGTERM and SIGINT and forward them as SIGTERM to the background processes
trap 'kill -TERM $BRAIN_PID; wait $BRAIN_PID' SIGTERM SIGINT

# Wait for brain.sh to complete
wait -n
