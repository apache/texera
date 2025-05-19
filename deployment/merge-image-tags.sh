#!/bin/bash
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


set -e

# Prompt for base tag
DEFAULT_TAG="latest"
read -p "Enter the base tag to merge [${DEFAULT_TAG}]: " BASE_TAG
BASE_TAG=${BASE_TAG:-$DEFAULT_TAG}

# Prompt for which services' manifests to merge
DEFAULT_SERVICES="*"
read -p "Enter services to merge (comma-separated, '*' for all) [${DEFAULT_SERVICES}]: " SERVICES_INPUT
SERVICES_INPUT=${SERVICES_INPUT:-$DEFAULT_SERVICES}

# Convert input into array for lookup
IFS=',' read -ra SELECTED_SERVICES <<< "$SERVICES_INPUT"

# Helper to check if a service should be merged
should_merge() {
  local svc="$1"
  if [[ "$SERVICES_INPUT" == "*" ]]; then
    return 0
  fi
  for sel in "${SELECTED_SERVICES[@]}"; do
    sel="$(echo -e "${sel}" | tr -d '[:space:]')"
    if [[ "$svc" == "$sel" ]]; then
      return 0
    fi
  done
  return 1
}

cd "$(dirname "$0")"

# Detect all Dockerfiles and extract service names
dockerfiles=( *.dockerfile )

if [[ ${#dockerfiles[@]} -eq 0 ]]; then
  echo "❌ No Dockerfiles found in the current directory."
  exit 1
fi

services=()
for file in "${dockerfiles[@]}"; do
  svc=$(basename "$file" .dockerfile)
  services+=("$svc")
done

# Add additional services that don't have a *.dockerfile in the deployment root
services+=("pylsp" "y-websocket-server")

echo "🔗 Merging multi-arch manifests for tag :$BASE_TAG"

for svc in "${services[@]}"; do
  # Skip if not selected by user
  if ! should_merge "$svc"; then
    echo "⏭️  Skipping $svc"
    continue
  fi
  echo "🔄 Creating manifest for texera/$svc:$BASE_TAG"
  docker buildx imagetools create \
    -t texera/$svc:$BASE_TAG \
    texera/$svc:${BASE_TAG}-amd64 \
    texera/$svc:${BASE_TAG}-arm64

  echo "✅ Created manifest: texera/$svc:$BASE_TAG"
done
