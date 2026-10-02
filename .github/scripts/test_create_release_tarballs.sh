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

# Regression tests for create-release-tarballs.sh. Builds the release archives
# for a throwaway repository twice, under a different umask, clock, time zone,
# working directory and SOURCE_DATE_EPOCH, and requires byte-identical output.
# Also pins the bundle's contents and the script's rejection of bad input.

set -uo pipefail

command -v python3 >/dev/null || { echo "python3 is required to run these tests" >&2; exit 1; }

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
create="$script_dir/create-release-tarballs.sh"
work="$(mktemp -d 2>/dev/null || mktemp -d -t release-tarballs)"
trap 'rm -rf "$work"' EXIT
rc=0

pass()   { echo "ok:   $1"; }
failed() { echo "FAIL: $1"; rc=1; }

VERSION="9.9.9-incubating"
SRC="apache-texera-${VERSION}-src.tar.gz"
COMPOSE="apache-texera-${VERSION}-docker-compose.tar.gz"

repo="$work/repo"
mkdir -p "$repo/bin/single-node/examples" "$repo/sql/updates"
(
  cd "$repo"
  git init -q
  git config user.email "test@example.com"
  git config user.name "test"
  printf 'services:\n  postgres:\n    volumes:\n      - ../../sql:/docker-entrypoint-initdb.d\n' \
    > bin/single-node/docker-compose.yml
  printf 'TEXERA_SERVICE_LOG_LEVEL=INFO\nIMAGE_TAG=latest\n' > bin/single-node/.env
  for f in nginx.conf litellm-config.yaml LICENSE DISCLAIMER README.md; do
    echo "$f" > "bin/single-node/$f"
  done
  printf '#!/usr/bin/env bash\necho load\n' > bin/single-node/examples/load-examples.sh
  chmod +x bin/single-node/examples/load-examples.sh
  echo "CREATE TABLE t();" > sql/texera_ddl.sql
  echo "ALTER TABLE t;" > "sql/updates/01-ünïcode.sql"
  echo "NOTICE" > NOTICE
  echo "not in the bundle" > README.md
  git add -A
  GIT_COMMITTER_DATE="2024-02-03T04:05:06Z" git commit -qm "release" --date="2024-02-03T04:05:06Z"
  git tag v9.9.9-incubating-rc1
  git tag -a -m "annotated" v9.9.9-incubating-rc1-annotated
)
commit_time=$(git -C "$repo" log -1 --format=%ct)

run() { (cd "$repo" && "$create" "$@") >"$work/run.log" 2>&1; }

# --- the same tag twice, with every host input that could leak changed ---
(umask 022; cd "$repo" && TZ=UTC "$create" v9.9.9-incubating-rc1 "$VERSION" ghcr.io/apache 1.0.0 "$work/out1") \
  >/dev/null 2>&1 || failed "first build should succeed"
sleep 1
(umask 077; cd "$repo/sql" && TZ=Asia/Tokyo SOURCE_DATE_EPOCH=1 \
  "$create" v9.9.9-incubating-rc1-annotated "$VERSION" ghcr.io/apache 1.0.0 "$work/out2") \
  >/dev/null 2>&1 || failed "second build should succeed"

for a in "$SRC" "$COMPOSE"; do
  if [[ -f "$work/out1/$a" ]] && cmp -s "$work/out1/$a" "$work/out2/$a"; then
    pass "$a is byte-identical across umask, clock, TZ, cwd and tag kind"
  else
    failed "$a differs between two builds of the same commit"
  fi
done

# --- archive metadata comes from the commit, not the machine ---
if python3 - "$work/out2" "$commit_time" "$SRC" "$COMPOSE" <<'EOF'
import gzip, sys, tarfile
out, commit_time, *names = sys.argv[1], int(sys.argv[2]), *sys.argv[3:]
for name in names:
    raw = open(f"{out}/{name}", "rb").read()
    assert raw[4:8] == b"\0\0\0\0", f"{name}: gzip header carries a timestamp"
    with tarfile.open(f"{out}/{name}") as tar:
        members = tar.getmembers()
        paths = [m.name for m in members]
        assert paths == sorted(paths), f"{name}: entries not sorted: {paths}"
        for m in members:
            assert m.mtime == commit_time, f"{m.name}: mtime {m.mtime} != {commit_time}"
            assert (m.uid, m.gid, m.uname, m.gname) == (0, 0, "", ""), f"{m.name}: owner leaked"
            expected = 0o755 if m.isdir() or m.name.endswith(".sh") else 0o644
            assert m.mode == expected, f"{m.name}: mode {oct(m.mode)} != {oct(expected)}"
EOF
then
  pass "entries are sorted, owned by 0:0, stamped with the commit time, umask-free"
else
  failed "archive metadata leaked from the build machine"
fi

# --- the compose bundle is assembled and patched as before ---
mkdir -p "$work/x" && tar -xzf "$work/out1/$COMPOSE" -C "$work/x"
b="$work/x/apache-texera-${VERSION}-docker-compose"
env_file="$(cat "$b/.env" 2>/dev/null)"
expected_env=$'TEXERA_SERVICE_LOG_LEVEL=ERROR\nIMAGE_TAG=1.0.0\nIMAGE_REGISTRY=ghcr.io/apache'
if [[ "$env_file" == "$expected_env" ]]; then
  pass ".env: IMAGE_TAG replaced, IMAGE_REGISTRY appended, log level forced to ERROR"
else
  failed ".env not patched as expected: $env_file"
fi
if grep -q -- '- ./sql:/docker-entrypoint-initdb.d' "$b/docker-compose.yml" && ! grep -q '\.\./\.\./sql' "$b/docker-compose.yml"; then
  pass "docker-compose.yml mounts ./sql"
else
  failed "docker-compose.yml sql mount not rewritten"
fi
if [[ -f "$b/sql/updates/01-ünïcode.sql" && -x "$b/examples/load-examples.sh" && -f "$b/NOTICE" && ! -e "$b/README.md.tmp" ]] \
  && [[ "$(cat "$b/README.md")" == "README.md" ]]; then
  pass "bundle carries sql/, examples/ (still executable), NOTICE and the single-node README"
else
  failed "bundle contents incomplete"
fi

# --- a different image tag changes only the compose bundle ---
run v9.9.9-incubating-rc1 "$VERSION" ghcr.io/apache 2.0.0 "$work/out3" || failed "third build should succeed"
if cmp -s "$work/out1/$SRC" "$work/out3/$SRC" && ! cmp -s "$work/out1/$COMPOSE" "$work/out3/$COMPOSE"; then
  pass "image tag affects the compose bundle only"
else
  failed "image tag should change the compose bundle and nothing else"
fi

# --- bad input is rejected before anything is written ---
expect_rejected() {
  local label="$1" want="$2"; shift 2
  rm -rf "$work/bad"
  run "$@"
  local got=$?
  if [[ "$got" -eq "$want" && ! -e "$work/bad/$SRC" && ! -e "$work/bad/$COMPOSE" ]]; then
    pass "$label -> exit $want, nothing written"
  else
    failed "$label should exit $want without output (got $got): $(cat "$work/run.log")"
  fi
}
expect_rejected "unknown tag" 1 v0.0.0-missing "$VERSION" ghcr.io/apache 1.0.0 "$work/bad"
expect_rejected "too few arguments" 2 v9.9.9-incubating-rc1 "$VERSION" ghcr.io/apache
expect_rejected "image tag with a sed delimiter" 2 v9.9.9-incubating-rc1 "$VERSION" ghcr.io/apache '1.0|x' "$work/bad"
expect_rejected "registry with a sed metacharacter" 2 v9.9.9-incubating-rc1 "$VERSION" 'ghcr.io/a&b' 1.0.0 "$work/bad"
expect_rejected "empty version" 2 v9.9.9-incubating-rc1 "" ghcr.io/apache 1.0.0 "$work/bad"
expect_rejected "version with a path separator" 2 v9.9.9-incubating-rc1 "9/../x" ghcr.io/apache 1.0.0 "$work/bad"
expect_rejected "image tag with a colon" 2 v9.9.9-incubating-rc1 "$VERSION" ghcr.io/apache 'a:b' "$work/bad"

if [[ "$rc" -ne 0 ]]; then
  echo "create-release-tarballs regression tests FAILED"
  exit 1
fi
echo "create-release-tarballs regression tests passed"
