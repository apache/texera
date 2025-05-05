#!/usr/bin/env bash
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

# Start computing unit master node in the background
bash scripts/workflow-computing-unit.sh &

# Wait for one of server.sh and computing unit master node to complete
wait -n
