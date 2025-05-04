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
