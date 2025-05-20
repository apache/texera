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
read -p "Enter the base tag for the images [${DEFAULT_TAG}]: " BASE_TAG
BASE_TAG=${BASE_TAG:-$DEFAULT_TAG}

# Prompt for which services to build
DEFAULT_SERVICES="*"
read -p "Enter services to build (comma-separated, '*' for all) [${DEFAULT_SERVICES}]: " SERVICES_INPUT
SERVICES_INPUT=${SERVICES_INPUT:-$DEFAULT_SERVICES}

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

# Detect platform
ARCH=$(uname -m)
if [[ "$ARCH" == "x86_64" ]]; then
  PLATFORM="linux/amd64"
  TAG_SUFFIX="amd64"
elif [[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]]; then
  PLATFORM="linux/arm64"
  TAG_SUFFIX="arm64"
else
  echo "❌ Unsupported architecture: $ARCH"
  exit 1
fi

FULL_TAG="${BASE_TAG}-${TAG_SUFFIX}"
echo "🔍 Detected architecture: $ARCH -> Building for $PLATFORM with tag :$FULL_TAG"

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

# Build pylsp service (directory: pylsp)
if should_build "pylsp"; then
  image="texera/pylsp:$FULL_TAG"
  echo "👉 Building $image from pylsp/Dockerfile"
  docker buildx build \
    --platform "$PLATFORM" \
    -f "pylsp/Dockerfile" \
    -t "$image" \
    --push \
    ./pylsp
fi

# Build y-websocket-server service (directory: y-websocket-server, image: y-websocket-server)
if should_build "y-websocket-server"; then
  image="texera/y-websocket-server:$FULL_TAG"
  echo "👉 Building $image from y-websocket-server/Dockerfile"
  docker buildx build \
    --platform "$PLATFORM" \
    -f "y-websocket-server/Dockerfile" \
    -t "$image" \
    --push \
    ./y-websocket-server
fi

echo "✅ All images built and pushed with tag :$FULL_TAG"
