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

# Push all Texera Docker images to Docker Hub.
# Usage: ./push-images.sh

set -euo pipefail

IMAGES=(
  "bobbai2000/texera-file-service:agent-service"
  "bobbai2000/texera-workflow-compiling-service:agent-service"
  "bobbai2000/texera-workflow-execution-coordinator:agent-service"
  "bobbai2000/texera-dashboard-service:agent-service"
  "bobbai2000/texera-workflow-computing-unit-managing-service:agent-service"
  "bobbai2000/texera-config-service:agent-service"
  "bobbai2000/texera-agent-service:agent-service"
)

echo "Pushing ${#IMAGES[@]} Texera images to Docker Hub..."
echo

for image in "${IMAGES[@]}"; do
  echo "==> Pushing $image"
  docker push "$image"
  echo
done

echo "Done. All images pushed successfully."
