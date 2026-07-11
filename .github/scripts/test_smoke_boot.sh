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

# End-to-end regression tests for smoke-boot.sh. Each case launches a fake
# "service" through smoke-boot.sh and checks its verdict from the exit code --
# smoke-boot no longer scans logs, so its decision is driven purely by whether
# the process reaches LISTEN, exits, or hangs.
#
# Guards issue #6332: a service that boots fine but prints exception-name prose
# in its log (e.g. jOOQ's random "tip of the day") must PASS; and a service that
# crashes on boot must still FAIL.

set -uo pipefail

command -v python3 >/dev/null || { echo "python3 is required to run these tests" >&2; exit 1; }

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
smoke="$script_dir/smoke-boot.sh"
work="$(mktemp -d 2>/dev/null || mktemp -d -t smoke-boot)"
trap 'rm -rf "$work"' EXIT
rc=0

# Print a likely-free localhost TCP port.
free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'
}

pass()   { echo "ok:   $1"; }
failed() { echo "FAIL: $1"; rc=1; }

# --- #6332: a healthy service that logs the jOOQ tip must PASS ---
# It prints prose naming NoClassDefFoundError / ClassNotFoundException, then
# opens its port and stays up. The old log-scanning check flagged this as a
# crash; the process-based check must not.
port="$(free_port)"
cat >"$work/healthy" <<EOF
#!/usr/bin/env bash
echo "jOOQ tip of the day: A NoClassDefFoundError or ClassNotFoundException is often a sign of a version mismatch"
exec python3 -c "import socket, time; s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(('127.0.0.1', $port)); s.listen(); time.sleep(30)"
EOF
chmod +x "$work/healthy"
if "$smoke" "$work/healthy" "$port" 15 >/dev/null 2>&1; then
  pass "healthy boot that logs the jOOQ tip -> OK (#6332)"
else
  failed "healthy boot that logs the jOOQ tip should be OK"
fi

# --- a service that crashes on boot (exits before listening) must FAIL ---
port="$(free_port)"
cat >"$work/crasher" <<EOF
#!/usr/bin/env bash
echo "startup failed" >&2
exit 1
EOF
chmod +x "$work/crasher"
if "$smoke" "$work/crasher" "$port" 15 >/dev/null 2>&1; then
  failed "crash on boot should FAIL"
else
  pass "crash on boot (exits before listening) -> FAIL"
fi

# --- a service that hangs without ever listening must FAIL (timeout) ---
port="$(free_port)"
cat >"$work/hang" <<EOF
#!/usr/bin/env bash
exec sleep 300
EOF
chmod +x "$work/hang"
if "$smoke" "$work/hang" "$port" 3 >/dev/null 2>&1; then
  failed "hang without listening should FAIL"
else
  pass "hang without listening -> FAIL (timeout)"
fi

# --- #6336: a crash must not be masked by a pre-existing listener on the port.
# Hold the port with an unrelated listener, then boot a crasher on it: the port
# is busy at launch, so smoke-boot must FAIL fast rather than mistake the
# squatter's LISTEN for a healthy boot. ---
port="$(free_port)"
python3 -c "import socket, time; s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(('127.0.0.1', $port)); s.listen(); time.sleep(60)" &
squatter=$!
# Wait until the squatter is actually accepting connections before testing.
for _ in $(seq 1 50); do
  python3 -c "import socket,sys; s=socket.socket(); s.settimeout(0.2); sys.exit(0 if s.connect_ex(('127.0.0.1',$port))==0 else 1)" && break
  sleep 0.1
done
if "$smoke" "$work/crasher" "$port" 5 >/dev/null 2>&1; then
  failed "a crash masked by a pre-existing listener should FAIL (#6336)"
else
  pass "port already in use -> FAIL fast, crash not masked (#6336)"
fi
kill "$squatter" 2>/dev/null || true
wait "$squatter" 2>/dev/null || true

if [[ "$rc" -ne 0 ]]; then
  echo "smoke-boot regression tests FAILED"
  exit 1
fi
echo "smoke-boot regression tests passed"
