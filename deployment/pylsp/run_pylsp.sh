#!/bin/bash
# Function to forcibly restart pylsp
restart_pylsp() {
  echo "Restarting pylsp..."
  # Kill the previous session of pylsp if it's still running
  [ ! -z "$PYLSP_PID" ] && kill -SIGTERM "$PYLSP_PID"
  # Start pylsp in a new session and redirect all output to stdout
  setsid pylsp --ws --port 3000 --verbose 2>&1 &
  PYLSP_PID=$!
}

# Setup traps for interruption and termination signals
trap 'kill -SIGTERM "$PYLSP_PID"; exit 0' SIGINT SIGTERM

# Start pylsp initially
restart_pylsp

# Main loop to restart pylsp every 5 minutes
while true; do
  # Wait for 10 minutes
  sleep 300
  # Restart pylsp
  restart_pylsp
done

