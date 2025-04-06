#!/bin/bash

set -e

# Prompt for base tag
DEFAULT_TAG="latest"
read -p "Enter the base tag to merge [${DEFAULT_TAG}]: " BASE_TAG
BASE_TAG=${BASE_TAG:-$DEFAULT_TAG}

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

echo "🔗 Merging multi-arch manifests for tag :$BASE_TAG"

for svc in "${services[@]}"; do
  echo "🔄 Creating manifest for texera/$svc:$BASE_TAG"
  docker buildx imagetools create \
    -t texera/$svc:$BASE_TAG \
    texera/$svc:${BASE_TAG}-amd64 \
    texera/$svc:${BASE_TAG}-arm64

  echo "✅ Created manifest: texera/$svc:$BASE_TAG"
done
