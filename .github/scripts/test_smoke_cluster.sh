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

# End-to-end regression tests for smoke-cluster.sh, driven by fake master/worker
# launchers (no real amber dist). These guard the harness's own decision logic:
#   - worker joins (master logs the 2-node readiness line)  -> PASS
#   - worker exits before joining                           -> FAIL ("worker exited...")
#   - worker never joins within the timeout                 -> FAIL ("did not join...")
#   - master dies during the join wait                      -> FAIL ("master exited...joined")
#   - master logs no "N nodes" line at all (readiness drift) -> FAIL (timeout + hint)
#   - master crashes before it listens                      -> FAIL ("...before listening")
# The failure cases assert the SPECIFIC error message, so a future edit that
# deletes a fast-fail branch (and lets the case pass only via the slow timeout)
# turns the test red instead of silently passing. The happy path ties the join
# signal to the worker actually running, so it can't pass if the worker is never
# launched. The real cluster join is exercised against the packaged dist in
# amber-integration. See https://github.com/apache/texera/issues/6523.

set -uo pipefail

command -v python3 >/dev/null || { echo "python3 is required to run these tests" >&2; exit 1; }

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
smoke="$script_dir/smoke-cluster.sh"
work="$(mktemp -d 2>/dev/null || mktemp -d -t smoke-cluster)"
trap 'rm -rf "$work"' EXIT
rc=0

PHRASE_SELF='Now we have 1 nodes in the cluster'
PHRASE_JOINED='Now we have 2 nodes in the cluster'

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}
pass()   { echo "ok:   $1"; }
failed() { echo "FAIL: $1"; rc=1; }

py_listen() {  # emit a python one-liner that binds $1 and holds it for $2 seconds
  echo "exec python3 -c \"import socket,time; s=socket.socket(); s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1); s.bind(('127.0.0.1',$1)); s.listen(); time.sleep($2)\""
}

# Fake master: binds $port so smoke-cluster's port-wait succeeds. $self=y prints
# the self-join line. $trigger is "n" (never emit the joined line), a number N
# (emit it N seconds after start), or "wait:<file>" (emit it only once <file>
# exists -- used to couple the join to the worker actually running). $hold is how
# long to hold the port (short => master dies mid-run).
#   $1=path $2=port $3=self(y/n) $4=trigger $5=hold_secs
make_master() {
  local path="$1" port="$2" self="$3" trigger="$4" hold="$5"
  {
    echo '#!/usr/bin/env bash'
    [[ "$self" == y ]] && echo "echo '$PHRASE_SELF'"
    case "$trigger" in
      n) : ;;
      wait:*) echo "( while [ ! -f '${trigger#wait:}' ]; do sleep 0.2; done; echo '$PHRASE_JOINED' ) &" ;;
      *) echo "( sleep $trigger; echo '$PHRASE_JOINED' ) &" ;;
    esac
    py_listen "$port" "$hold"
  } >"$path"
  chmod +x "$path"
}

# Fake worker: sleeps (healthy), crashes on boot, or touches a signal file (to
# trigger a coupled master) then sleeps.  $1=path $2=sleep|crash|touch:<file>
make_worker() {
  case "$2" in
    crash)   printf '#!/usr/bin/env bash\necho "worker boom" >&2\nexit 1\n' >"$1" ;;
    touch:*) printf '#!/usr/bin/env bash\ntouch "%s"\nexec sleep 120\n' "${2#touch:}" >"$1" ;;
    *)       printf '#!/usr/bin/env bash\nexec sleep 120\n' >"$1" ;;
  esac
  chmod +x "$1"
}

# Assert smoke-cluster FAILS and its output contains $needle (locks in the branch
# that produces that message). $1=desc $2=needle, remaining args = smoke-cluster argv.
assert_fail_msg() {
  local desc="$1" needle="$2"; shift 2
  local out
  if out="$("$smoke" "$@" 2>&1)"; then
    failed "$desc: expected FAIL but it passed"
  elif printf '%s\n' "$out" | grep -Fq "$needle"; then
    pass "$desc"
  else
    failed "$desc: failed but message missing '$needle'"; printf '%s\n' "$out" | tail -n3
  fi
}

# --- happy path: the worker triggers the join; master observes 2 nodes -> PASS.
# The joined line fires only after the worker touches the signal, so this cannot
# pass if smoke-cluster never launches the worker. ---
port="$(free_port)"; sig="$work/joined.signal"
make_master "$work/m_ok" "$port" y "wait:$sig" 120
make_worker "$work/w_ok" "touch:$sig"
if "$smoke" "$work/m_ok" "$work/w_ok" "$port" 30 >/dev/null 2>&1; then
  pass "worker joins cluster (master sees 2 nodes) -> OK"
else
  failed "worker join should be OK"
fi

# --- worker exits before joining -> FAIL fast (assert the fast-fail message) ---
port="$(free_port)"
make_master "$work/m_nojoin" "$port" y n 120
make_worker "$work/w_crash" crash
assert_fail_msg \
  "worker exits before join -> FAIL fast" "worker exited before joining" \
  "$work/m_nojoin" "$work/w_crash" "$port" 15

# --- worker never joins within the timeout -> FAIL (assert the timeout message) ---
port="$(free_port)"
make_master "$work/m_nojoin2" "$port" y n 120
make_worker "$work/w_sleep" sleep
assert_fail_msg \
  "worker never joins -> FAIL (timeout)" "did not join the cluster within" \
  "$work/m_nojoin2" "$work/w_sleep" "$port" 3

# --- master dies during the join wait -> FAIL (assert the master-death message) ---
port="$(free_port)"
make_master "$work/m_die" "$port" y n 4
make_worker "$work/w_sleep_b" sleep
assert_fail_msg \
  "master dies during join wait -> FAIL" "master exited before the worker joined" \
  "$work/m_die" "$work/w_sleep_b" "$port" 20

# --- master logs no "N nodes" line at all (readiness log drift) -> FAIL with a hint ---
port="$(free_port)"
make_master "$work/m_drift" "$port" n n 120
make_worker "$work/w_sleep2" sleep
assert_fail_msg \
  "master readiness log drift -> FAIL (timeout + drift hint)" "readiness log may have changed" \
  "$work/m_drift" "$work/w_sleep2" "$port" 3

# --- master crashes before it listens -> FAIL (assert the pre-listen message) ---
port="$(free_port)"
printf '#!/usr/bin/env bash\necho "master boom" >&2\nexit 1\n' >"$work/m_crash"; chmod +x "$work/m_crash"
make_worker "$work/w_sleep3" sleep
SMOKE_MASTER_READY_TIMEOUT=15 assert_fail_msg \
  "master crashes before listening -> FAIL" "master exited before listening" \
  "$work/m_crash" "$work/w_sleep3" "$port" 20

if [[ "$rc" -ne 0 ]]; then
  echo "smoke-cluster regression tests FAILED"
  exit 1
fi
echo "smoke-cluster regression tests passed"
