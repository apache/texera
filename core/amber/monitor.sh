#!/bin/bash

# Configuration
PROCESS_NAME_OR_PID="$1"   # Can be process name or PID
WATCH_FOLDER="$2"
INTERVAL=5  # Seconds

if [[ -z "$PROCESS_NAME_OR_PID" || -z "$WATCH_FOLDER" ]]; then
  echo "Usage: $0 <process_name_or_pid> <folder_path>"
  exit 1
fi

echo "Monitoring memory usage of '$PROCESS_NAME_OR_PID' and disk usage of '$WATCH_FOLDER' every $INTERVAL seconds..."
echo "Press Ctrl+C to stop."

while true; do
  # Get memory usage in MB
  if [[ "$PROCESS_NAME_OR_PID" =~ ^[0-9]+$ ]]; then
    # It's a PID
    MEM_MB=$(ps -o rss= -p "$PROCESS_NAME_OR_PID" 2>/dev/null | awk '{printf "%.2f", $1/1024}')
  else
    # It's a process name
    MEM_MB=$(ps -caxm -o command,rss | grep "$PROCESS_NAME_OR_PID" | grep -v grep | awk '{sum += $NF} END {printf "%.2f", sum/1024}')
  fi

  # Get disk usage in MB
  DISK_MB=$(du -sm "$WATCH_FOLDER" 2>/dev/null | awk '{print $1}')

  TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
  echo "[$TIMESTAMP] Memory: ${MEM_MB:-N/A} MB | Disk: ${DISK_MB:-N/A} MB"

  sleep "$INTERVAL"
done

