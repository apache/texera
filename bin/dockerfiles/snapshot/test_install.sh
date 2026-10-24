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

# Tests for install.sh's pure logic: snapshot date handling, the archive
# sources it points apt at, and how it splits requirements between PyPI and an
# extra index. The installs themselves are exercised by the image builds.

set -uo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../../.." && pwd)"
# shellcheck source=install.sh
source "$script_dir/install.sh"
set +e
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
rc=0

pass()   { echo "ok:   $1"; }
failed() { echo "FAIL: $1"; rc=1; }
expect_eq() {
  if [[ "$2" == "$3" ]]; then pass "$1"; else failed "$1: expected [$3], got [$2]"; fi
}

# --- snapshot_times ---
expect_eq "unset PACKAGE_SNAPSHOT -> live archives" "$(unset PACKAGE_SNAPSHOT; snapshot_times)" ""
expect_eq "empty PACKAGE_SNAPSHOT -> live archives" "$(PACKAGE_SNAPSHOT= snapshot_times)" ""
expect_eq "date -> apt stamp, pip cutoff and epoch" "$(PACKAGE_SNAPSHOT=2026-09-30 snapshot_times)" \
  "20260930T000000Z 2026-09-30T00:00:00Z 1790726400"
expect_eq "read in UTC whatever the build host's zone" \
  "$(TZ=Pacific/Kiritimati PACKAGE_SNAPSHOT=1970-01-01 snapshot_times)" "19700101T000000Z 1970-01-01T00:00:00Z 0"
expect_eq "leap day accepted" "$(PACKAGE_SNAPSHOT=2028-02-29 snapshot_times | cut -d' ' -f1)" "20280229T000000Z"
for bad in 2026-9-30 2026-02-30 2027-02-29 20260930 2026-09-30T00:00 " 2026-09-30" 2026-13-01 x; do
  out=$(PACKAGE_SNAPSHOT="$bad" snapshot_times 2>&1)
  if [[ $? -ne 0 && "$out" == *"PACKAGE_SNAPSHOT must be a UTC date"* ]]; then
    pass "rejects PACKAGE_SNAPSHOT [$bad]"
  else
    failed "should reject PACKAGE_SNAPSHOT [$bad], got [$out]"
  fi
done

# --- snapshot_sources ---
printf 'ID=ubuntu\nVERSION_CODENAME=jammy\n' > "$work/ubuntu"
expect_eq "ubuntu: all three suites from one snapshot (any architecture)" \
  "$(snapshot_sources "$work/ubuntu" 20260930T000000Z)" \
  "deb https://snapshot.ubuntu.com/ubuntu/20260930T000000Z jammy main restricted universe multiverse
deb https://snapshot.ubuntu.com/ubuntu/20260930T000000Z jammy-updates main restricted universe multiverse
deb https://snapshot.ubuntu.com/ubuntu/20260930T000000Z jammy-security main restricted universe multiverse"
printf 'ID=debian\nVERSION_CODENAME=bookworm\n' > "$work/debian"
debian_sources="$(snapshot_sources "$work/debian" 20260930T000000Z)"
expect_eq "debian: security suite from the debian-security snapshot" \
  "$(grep -c 'archive/debian-security/20260930T000000Z bookworm-security main' <<< "$debian_sources")" "1"
expect_eq "debian: every line skips Valid-Until and names the keyring" \
  "$(grep -c '^deb \[check-valid-until=no signed-by=/usr/share/keyrings/debian-archive-keyring.gpg\] http://' <<< "$debian_sources")" "3"
printf 'ID=alpine\nVERSION_CODENAME=\n' > "$work/alpine"
if out=$(snapshot_sources "$work/alpine" 20260930T000000Z 2>&1); then
  failed "unknown distribution should fail"
else
  expect_eq "unknown distribution -> error" "$out" "No archive snapshot known for distribution 'alpine'"
fi

# --- split_requirements ---
cat > "$work/req.txt" <<'EOF'
# comment
numpy==2.1.0   # inline comment
--extra-index-url https://download.pytorch.org/whl/cpu

torch==2.13.0+cpu ; platform_system == "Linux" and platform_machine == "x86_64"
torch==2.13.0 ; platform_system != "Linux" or platform_machine != "x86_64"
odd==1.0 ; extra == "a+b"
EOF
printf -- '--index-url=https://example.org/simple\nwheel==0.45.1' > "$work/req2.txt"
mkdir "$work/split"
if split_requirements "$work/split" "$work/req.txt" "$work/req2.txt"; then
  expect_eq "local versions (and only those) go to the extra index" "$(cat "$work/split/local.txt")" \
    'torch==2.13.0+cpu ; platform_system == "Linux" and platform_machine == "x86_64"'
  expect_eq "every requirement stays in the PyPI pass" "$(wc -l < "$work/split/pypi.txt")" "5"
  expect_eq "a '+' in a marker is not a local version" "$(grep -c '^odd' "$work/split/local.txt")" "0"
  expect_eq "index options are collected, not passed to PyPI" "$(cat "$work/split/indexes")" \
    "https://download.pytorch.org/whl/cpu
https://example.org/simple"
  expect_eq "a last line without a newline is kept" "$(tail -n1 "$work/split/pypi.txt")" "wheel==0.45.1"
else
  failed "split_requirements failed on valid input"
fi
printf 'numpy==2.1.0\n-r other.txt\n' > "$work/nested.txt"
if out=$(split_requirements "$work/split" "$work/nested.txt" 2>&1); then
  failed "nested -r should be rejected"
else
  pass "rejects options it cannot carry over (nested -r): $out"
fi
mkdir "$work/real"
split_requirements "$work/real" "$repo_root/amber/requirements.txt" "$repo_root/amber/operator-requirements.txt"
expect_eq "the repo's requirements: only the +cpu torch needs the PyTorch index" \
  "$(sed 's/ *;.*//' "$work/real/local.txt")" "torch==2.13.0+cpu"

# --- pip invocations (python3 stubbed to record what it was asked to run) ---
python3() { echo "hashseed=${PYTHONHASHSEED-unset} sde=${SOURCE_DATE_EPOCH-unset} $*"; }
expect_eq "without a snapshot pip runs as before" "$(unset PACKAGE_SNAPSHOT; pip_snapshot wheel)" \
  "hashseed=unset sde=unset -m pip install --no-cache-dir wheel"
expect_eq "with a snapshot pip gets the cutoff and deterministic byte-compilation" \
  "$(PACKAGE_SNAPSHOT=2026-09-30 pip_snapshot wheel)" \
  "hashseed=0 sde=1790726400 -m pip install --no-cache-dir --uploaded-prior-to 2026-09-30T00:00:00Z wheel"
expect_eq "the pip bootstrap is byte-compiled deterministically too" \
  "$(PACKAGE_SNAPSHOT=2026-09-30 pip_bootstrap)" \
  "hashseed=0 sde=1790726400 -m pip install --no-cache-dir --no-deps -r $script_dir/pip-bootstrap-requirements.txt"
pip_calls="$(PACKAGE_SNAPSHOT=2026-09-30 pip_requirements "$work/req.txt")"
expect_eq "local pins: from the extra index, exact, no dependencies, no cutoff" "$(sed -n 1p <<< "$pip_calls")" \
  "hashseed=0 sde=1790726400 -m pip install --no-cache-dir --no-deps --extra-index-url https://download.pytorch.org/whl/cpu -r $(sed -n 1p <<< "$pip_calls" | sed 's/.* -r //')"
expect_eq "then everything else from PyPI under the cutoff" "$(sed -n 2p <<< "$pip_calls" | sed 's/ -r .*//')" \
  "hashseed=0 sde=1790726400 -m pip install --no-cache-dir --uploaded-prior-to 2026-09-30T00:00:00Z"
expect_eq "without local pins only the PyPI pass runs" \
  "$(printf 'numpy==2.1.0\n' > "$work/plain.txt"; PACKAGE_SNAPSHOT=2026-09-30 pip_requirements "$work/plain.txt" | wc -l)" "1"
expect_eq "without a snapshot the files go to pip untouched" \
  "$(unset PACKAGE_SNAPSHOT; pip_requirements "$work/req.txt" "$work/plain.txt")" \
  "hashseed=unset sde=unset -m pip install --no-cache-dir -r $work/req.txt -r $work/plain.txt"
unset -f python3

# --- main ---
out=$(main bogus 2>&1)
expect_eq "unknown command -> usage, exit 2" "$? ${out%% *}" "2 Usage:"

if [[ "$rc" -ne 0 ]]; then
  echo "snapshot install.sh tests FAILED"
  exit 1
fi
echo "snapshot install.sh tests passed"
