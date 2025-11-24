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

SERVICES_INPUT="*"

# Convert the user input into an array for easy lookup
IFS=',' read -ra SELECTED_SERVICES <<< "$SERVICES_INPUT"

# Helper to determine whether a given service should be built
should_build() {
  local svc="$1"
  # Build everything if the user specified '*'
  if [[ "$SERVICES_INPUT" == "*" ]]; then
    return 0
  fi
  for sel in "${SELECTED_SERVICES[@]}"; do
    # Trim possible whitespace around each token
    sel="$(echo -e "${sel}" | tr -d '[:space:]')"
    if [[ "$svc" == "$sel" ]]; then
      return 0
    fi
  done
  return 1
}

PLATFORM="linux/amd64"
FULL_TAG="admap-deployment-amd64"

# Ensure Buildx is ready
docker buildx create --name texera-builder --use --bootstrap > /dev/null 2>&1 || docker buildx use texera-builder

cd "$(dirname "$0")"

# Auto-detect Dockerfiles in current directory
dockerfiles=( *.dockerfile )

if [[ ${#dockerfiles[@]} -eq 0 ]]; then
  echo "❌ No Dockerfiles found (*.dockerfile) in the current directory."
  exit 1
fi

echo "🔨 Building and pushing Texera images for $PLATFORM..."

for dockerfile in "${dockerfiles[@]}"; do
  service_name=$(basename "$dockerfile" .dockerfile)

  # Skip services the user did not ask for
  if ! should_build "$service_name"; then
    echo "⏭️  Skipping $service_name"
    continue
  fi

  image="texera/$service_name:$FULL_TAG"

  echo "👉 Building $image from $dockerfile"

  docker buildx build \
    --platform "$PLATFORM" \
    -f "$dockerfile" \
    -t "$image" \
    --push \
    ..
done

echo "✅ All images built and pushed with tag :$FULL_TAG"
