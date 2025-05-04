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
