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

# Installs the OS and Python packages of the image builds as they stood at
# 00:00 UTC on PACKAGE_SNAPSHOT (YYYY-MM-DD), so rebuilding a commit installs
# the same versions no matter how much later it runs. apt reads the
# snapshot.ubuntu.com / snapshot.debian.org copy of the archive for that
# moment; pip only considers files uploaded to PyPI before it. Without
# PACKAGE_SNAPSHOT it installs from the live archives and PyPI.
#
# Bind-mount the directory rather than COPY it, so it never lands in a layer:
#   ARG PACKAGE_SNAPSHOT
#   RUN --mount=type=bind,source=bin/dockerfiles/snapshot,target=/snapshot \
#       bash /snapshot/install.sh apt [apt-get install option]... <package>...
#   ... bash /snapshot/install.sh pip <pip install argument>...
#   ... bash /snapshot/install.sh pip-requirements <requirements file>...

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Prints the snapshot as "<apt timestamp> <pip timestamp> <epoch seconds>", or
# nothing when PACKAGE_SNAPSHOT is unset.
snapshot_times() {
  local snapshot="${PACKAGE_SNAPSHOT:-}"
  [[ -z "$snapshot" ]] && return 0
  local epoch
  if [[ ! "$snapshot" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] \
    || ! epoch=$(date -u -d "${snapshot}T00:00:00Z" +%s 2>/dev/null) \
    || [[ "$(date -u -d "@$epoch" +%F)" != "$snapshot" ]]; then
    echo "PACKAGE_SNAPSHOT must be a UTC date (YYYY-MM-DD), got '$snapshot'" >&2
    return 1
  fi
  echo "${snapshot//-/}T000000Z ${snapshot}T00:00:00Z ${epoch}"
}

# Prints a one-line-style sources.list pointing the distribution in
# /etc/os-release (or $1) at the archive snapshot taken at $2.
snapshot_sources() {
  local os_release="$1" stamp="$2" id codename
  id=$(. "$os_release" && echo "$ID")
  codename=$(. "$os_release" && echo "$VERSION_CODENAME")
  case "$id" in
    ubuntu)
      # One snapshot serves every architecture, ports (arm64) included.
      local base="https://snapshot.ubuntu.com/ubuntu/${stamp}"
      for suite in "$codename" "${codename}-updates" "${codename}-security"; do
        echo "deb ${base} ${suite} main restricted universe multiverse"
      done
      ;;
    debian)
      # Plain http: slim images have no CA bundle before this install, and apt
      # checks the signed InRelease either way. Snapshots outlive Valid-Until.
      local opts="[check-valid-until=no signed-by=/usr/share/keyrings/debian-archive-keyring.gpg]"
      echo "deb ${opts} http://snapshot.debian.org/archive/debian/${stamp} ${codename} main"
      echo "deb ${opts} http://snapshot.debian.org/archive/debian/${stamp} ${codename}-updates main"
      echo "deb ${opts} http://snapshot.debian.org/archive/debian-security/${stamp} ${codename}-security main"
      ;;
    *)
      echo "No archive snapshot known for distribution '$id'" >&2
      return 1
      ;;
  esac
}

# apt-get writes logs, solver dumps and caches that record when it ran.
apt_cleanup() {
  apt-get clean
  rm -rf /var/lib/apt/lists/* /var/log/apt/* /var/log/dpkg.log /var/log/alternatives.log \
    /var/cache/ldconfig/aux-cache /var/cache/debconf/*-old /var/lib/dpkg/*-old
}

apt_install() {
  local times opts=()
  times=$(snapshot_times)
  if [[ -n "$times" ]]; then
    local work
    work=$(mktemp -d)
    # shellcheck disable=SC2064
    trap "rm -rf '$work'" RETURN
    snapshot_sources /etc/os-release "${times%% *}" > "$work/sources.list"
    mkdir "$work/sources.list.d"
    # An alternate source list leaves the image's own apt sources untouched,
    # so later `apt-get install`s in derived images still use the live archive.
    opts=(-o "Dir::Etc::SourceList=$work/sources.list" -o "Dir::Etc::SourceParts=$work/sources.list.d")
  fi
  apt-get "${opts[@]}" update
  DEBIAN_FRONTEND=noninteractive apt-get "${opts[@]}" install -y "$@"
  apt_cleanup
}

# Splits requirements files into the pins that can only come from an extra
# index (a local version such as torch==2.13.0+cpu, which PyPI rejects) and
# everything else. Writes <dir>/local.txt, <dir>/pypi.txt and <dir>/indexes,
# the extra index URLs. The local pins stay in pypi.txt too, where the
# already-installed wheel satisfies them and pip resolves their dependencies.
split_requirements() {
  local dir="$1"
  shift
  : > "$dir/local.txt"
  : > "$dir/pypi.txt"
  : > "$dir/indexes"
  local file line spec
  for file in "$@"; do
    while IFS= read -r line || [[ -n "$line" ]]; do
      spec="${line%%#*}"
      if [[ "$spec" =~ ^[[:space:]]*--(extra-)?index-url[[:space:]=]+([^[:space:]]+) ]]; then
        echo "${BASH_REMATCH[2]}" >> "$dir/indexes"
        continue
      fi
      [[ "$spec" =~ ^[[:space:]]*$ ]] && continue
      if [[ "$spec" =~ ^[[:space:]]*- ]]; then
        echo "Unsupported option in $file: $spec" >&2
        return 1
      fi
      echo "$spec" >> "$dir/pypi.txt"
      if [[ "${spec%%;*}" =~ ==[^[:space:]]*\+ ]]; then
        echo "$spec" >> "$dir/local.txt"
      fi
    done < "$file"
  done
}

# Runs `pip install`. With a snapshot, byte-compilation is made deterministic:
# SOURCE_DATE_EPOCH switches .pyc files to hash-based invalidation (no source
# mtime in the header), and a fixed hash seed fixes set ordering in them.
pip_install() {
  local times
  times=$(snapshot_times)
  if [[ -z "$times" ]]; then
    python3 -m pip install --no-cache-dir "$@"
  else
    PYTHONHASHSEED=0 SOURCE_DATE_EPOCH="${times##* }" python3 -m pip install --no-cache-dir "$@"
  fi
}

pip_bootstrap() {
  # The distribution's pip predates --uploaded-prior-to.
  pip_install --no-deps -r "$here/pip-bootstrap-requirements.txt"
}

# pip install, considering only files uploaded before the snapshot.
pip_snapshot() {
  local times
  times=$(snapshot_times)
  if [[ -z "$times" ]]; then
    pip_install "$@"
  else
    local rest="${times#* }"
    pip_install --uploaded-prior-to "${rest%% *}" "$@"
  fi
}

pip_requirements() {
  local times
  times=$(snapshot_times)
  if [[ -z "$times" ]]; then
    local args=()
    for f in "$@"; do args+=(-r "$f"); done
    pip_install "${args[@]}"
    return
  fi
  local work
  work=$(mktemp -d)
  # shellcheck disable=SC2064
  trap "rm -rf '$work'" RETURN
  split_requirements "$work" "$@"
  if [[ -s "$work/local.txt" ]]; then
    # Extra indexes such as download.pytorch.org publish no upload times, so
    # the cutoff cannot apply there; an exact pin of a local version is the
    # one file it can resolve to, which is what makes this reproducible.
    local index_args=() url
    while IFS= read -r url; do index_args+=(--extra-index-url "$url"); done < "$work/indexes"
    pip_install --no-deps "${index_args[@]}" -r "$work/local.txt"
  fi
  pip_snapshot -r "$work/pypi.txt"
}

main() {
  local cmd="${1:-}"
  shift || true
  case "$cmd" in
    apt) apt_install "$@" ;;
    pip) pip_bootstrap && pip_snapshot "$@" ;;
    pip-requirements) pip_bootstrap && pip_requirements "$@" ;;
    *)
      echo "Usage: $0 {apt|pip|pip-requirements} <arguments>..." >&2
      return 2
      ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
