# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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

