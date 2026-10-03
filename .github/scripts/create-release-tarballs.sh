#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds the release source tarball and Docker Compose bundle from a git tag.
# Both archives are reproducible: the same tag and arguments give the same
# bytes on any machine, so a voter can rebuild them and compare the checksums
# against the staged artifacts. Everything the archives would otherwise pick up
# from the build machine (clock, umask, user, file order, locale) is pinned to
# the tagged commit or to a constant.
#
# Usage: create-release-tarballs.sh <tag> <version> <image-registry> <image-tag> [output-dir]
# Needs GNU tar (gtar on macOS) and gzip. Writes
#   apache-texera-<version>-src.tar.gz
#   apache-texera-<version>-docker-compose.tar.gz
# to output-dir (default: the current directory).

set -euo pipefail

if [[ $# -lt 4 || $# -gt 5 ]]; then
  echo "Usage: $0 <tag> <version> <image-registry> <image-tag> [output-dir]" >&2
  exit 2
fi

tag="$1"
version="$2"
image_registry="$3"
image_tag="$4"
out_dir="${5:-.}"

export LC_ALL=C TZ=UTC

# These land in file names and in a sed replacement, so anything outside the
# characters a version, image tag or registry path can contain is rejected.
check_arg() {
  if [[ ! "$2" =~ $3 ]]; then
    echo "Error: invalid $1 '$2'" >&2
    exit 2
  fi
}
check_arg version "$version" '^[A-Za-z0-9][A-Za-z0-9._-]*$'
check_arg image-tag "$image_tag" '^[A-Za-z0-9_][A-Za-z0-9._-]*$'
check_arg image-registry "$image_registry" '^[A-Za-z0-9][A-Za-z0-9._:/-]*$'

if tar --version 2>/dev/null | grep -q 'GNU tar'; then
  gnu_tar=tar
elif command -v gtar >/dev/null && gtar --version | grep -q 'GNU tar'; then
  gnu_tar=gtar
else
  echo "Error: GNU tar is required (on macOS: brew install gnu-tar)" >&2
  exit 1
fi

if ! commit=$(git rev-parse --verify --quiet "${tag}^{commit}"); then
  echo "Error: '${tag}' does not name a commit" >&2
  exit 1
fi
mkdir -p "$out_dir"
out_dir=$(cd "$out_dir" && pwd)
# git archive resolves pathspecs against the working directory.
cd "$(git rev-parse --show-toplevel)"

# Derived from the commit rather than read from the environment, so a stray
# SOURCE_DATE_EPOCH in a voter's shell cannot change the result.
SOURCE_DATE_EPOCH=$(git log -1 --format=%ct "$commit")
export SOURCE_DATE_EPOCH

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# Recipe from https://reproducible-builds.org/docs/archives/. --mode strips the
# umask's influence on permission bits; gzip -n keeps the input name and time
# out of the gzip header.
reproducible_tar_gz() {
  local parent="$1" dir="$2" output="$3"
  "$gnu_tar" --sort=name --format=pax \
    --mtime="@${SOURCE_DATE_EPOCH}" \
    --owner=0 --group=0 --numeric-owner \
    --mode='u+rwX,go+rX,go-w' \
    --pax-option='exthdr.name=%d/PaxHeaders/%f,delete=atime,delete=ctime' \
    -C "$parent" -cf - "$dir" | gzip -9 -n > "$output"
}

# Replaces KEY's line in an env file, or appends it when absent. Writes through
# a temp file instead of `sed -i`, whose flags differ between GNU and BSD sed.
set_env_var() {
  local file="$1" key="$2" value="$3"
  if grep -q "^${key}=" "$file"; then
    sed "s|^${key}=.*|${key}=${value}|" "$file" > "$file.tmp"
    mv "$file.tmp" "$file"
  else
    echo "${key}=${value}" >> "$file"
  fi
}

src_name="apache-texera-${version}-src"
mkdir -p "$work/src"
git archive --format=tar --prefix="${src_name}/" "$commit" | "$gnu_tar" -x -C "$work/src"
reproducible_tar_gz "$work/src" "$src_name" "$out_dir/${src_name}.tar.gz"

compose_name="apache-texera-${version}-docker-compose"
raw="$work/raw"
bundle="$work/compose/$compose_name"
mkdir -p "$raw" "$bundle"
git archive --format=tar "$commit" -- bin/single-node/ sql/ NOTICE | "$gnu_tar" -x -C "$raw"

for f in docker-compose.yml nginx.conf litellm-config.yaml LICENSE DISCLAIMER README.md .env; do
  cp "$raw/bin/single-node/$f" "$bundle/"
done
cp "$raw/NOTICE" "$bundle/"
cp -R "$raw/sql" "$bundle/"
if [[ -d "$raw/bin/single-node/examples" ]]; then
  cp -R "$raw/bin/single-node/examples" "$bundle/"
fi

# The repo mounts ../../sql relative to bin/single-node/; the bundle keeps sql/
# next to docker-compose.yml.
sed 's|\.\./\.\./sql|./sql|g' "$bundle/docker-compose.yml" > "$bundle/docker-compose.yml.tmp"
mv "$bundle/docker-compose.yml.tmp" "$bundle/docker-compose.yml"

set_env_var "$bundle/.env" IMAGE_REGISTRY "$image_registry"
set_env_var "$bundle/.env" IMAGE_TAG "$image_tag"
set_env_var "$bundle/.env" TEXERA_SERVICE_LOG_LEVEL ERROR

reproducible_tar_gz "$work/compose" "$compose_name" "$out_dir/${compose_name}.tar.gz"

echo "Created ${out_dir}/${src_name}.tar.gz"
echo "Created ${out_dir}/${compose_name}.tar.gz"
echo "SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH} (commit ${commit})"
