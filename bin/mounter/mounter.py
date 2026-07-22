#!/usr/bin/env python3
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

"""
texera-mounter: a per-node privileged service that performs GeeseFS FUSE mounts on behalf
of (unprivileged) computing-unit pods.

A CU pod POSTs /mount with {cuid, repositoryName, commitHash, jwt, fileServiceBase}. The
mounter runs GeeseFS against file-service's JWT-authenticated S3 proxy (passing the pod's
JWT as the S3 access key) and mounts read-only under MOUNT_ROOT/<cuid>/<repo>/<commit>.
That host directory is bind-mounted (mountPropagation: Bidirectional) into the mounter and
propagates back into the CU pod (mountPropagation: HostToContainer), so the CU pod sees
the mount without any privilege of its own.

The mounter holds no LakeFS credentials; authorization stays entirely in file-service.
A background reaper unmounts directories whose owning CU pod no longer exists.
"""

import json
import os
import shutil
import ssl
import subprocess
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MOUNT_ROOT = os.environ.get("MOUNT_ROOT", "/var/lib/texera-mounts")
MOUNTER_PORT = int(os.environ.get("MOUNTER_PORT", "8100"))
POOL_NAMESPACE = os.environ.get("POOL_NAMESPACE", "texera-workflow-computing-unit-pool")
MOUNT_SECRET_PLACEHOLDER = "texera-jwt-mount"
MOUNT_TIMEOUT_S = 30
REAPER_INTERVAL_S = 60

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"


def log(msg):
    print(f"[mounter] {msg}", flush=True)


def ensure_shared_root():
    """Make MOUNT_ROOT a shared mount so mounts created under it propagate to peers."""
    os.makedirs(MOUNT_ROOT, exist_ok=True)
    if not os.path.ismount(MOUNT_ROOT):
        subprocess.run(["mount", "--bind", MOUNT_ROOT, MOUNT_ROOT], check=False)
    subprocess.run(["mount", "--make-rshared", MOUNT_ROOT], check=False)


def do_mount(cuid, repo, commit, jwt, file_service_base):
    """Idempotently mount repo:commit for cuid. Returns the mount target path."""
    if not cuid or not repo or not commit or not jwt or not file_service_base:
        raise ValueError("cuid, repositoryName, commitHash, jwt and fileServiceBase are required")

    target = os.path.join(MOUNT_ROOT, cuid, repo, commit)
    if os.path.ismount(target):
        log(f"{repo}:{commit} already mounted for cu {cuid} at {target}")
        return target

    os.makedirs(target, exist_ok=True)
    # allow_other: the mounter runs as root but the CU pod's UDF runs as a different
    # (non-root) user, so the propagated FUSE mount must permit other users to access it.
    cmd = [
        "geesefs",
        "--endpoint", file_service_base,
        "--memory-limit", "512",
        "-o", "ro,allow_other",
        f"{repo}:{commit}",
        target,
    ]
    env = dict(os.environ)
    env["AWS_ACCESS_KEY_ID"] = jwt
    env["AWS_SECRET_ACCESS_KEY"] = MOUNT_SECRET_PLACEHOLDER
    log(f"mounting {repo}:{commit} for cu {cuid} via: geesefs --endpoint {file_service_base} ... {target}")
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"geesefs exited {result.returncode}: {(result.stdout + result.stderr).strip()}"
        )

    # GeeseFS daemonizes after a successful mount; wait until the kernel reports it.
    deadline = time.time() + MOUNT_TIMEOUT_S
    while not os.path.ismount(target):
        if time.time() > deadline:
            raise RuntimeError(f"{repo}:{commit} did not appear as a mount at {target} in {MOUNT_TIMEOUT_S}s")
        time.sleep(0.2)
    log(f"mounted {repo}:{commit} for cu {cuid} at {target}")
    return target


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/healthz":
            self._send(200, {"status": "ok"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/mount":
            self._send(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            req = json.loads(self.rfile.read(length) or b"{}")
            target = do_mount(
                str(req.get("cuid", "")),
                str(req.get("repositoryName", "")),
                str(req.get("commitHash", "")),
                str(req.get("jwt", "")),
                str(req.get("fileServiceBase", "")),
            )
            self._send(200, {"mountPath": target})
        except ValueError as e:
            self._send(400, {"error": str(e)})
        except Exception as e:  # noqa: BLE001
            log(f"mount failed: {e}")
            self._send(500, {"error": str(e)})

    def log_message(self, fmt, *args):  # silence default per-request stderr logging
        pass


# ---- reaper: unmount directories whose owning CU pod is gone ----

def _k8s_get(path):
    try:
        with open(os.path.join(SA_DIR, "token")) as f:
            token = f.read().strip()
    except OSError:
        return None  # not running in-cluster; skip reaping
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    url = f"https://{host}:{port}{path}"
    ctx = ssl.create_default_context(cafile=os.path.join(SA_DIR, "ca.crt"))
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req, timeout=10, context=ctx) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:  # noqa: BLE001
        log(f"reaper k8s query failed: {e}")
        return None


def _cu_pod_exists(cuid):
    status = _k8s_get(f"/api/v1/namespaces/{POOL_NAMESPACE}/pods/computing-unit-{cuid}")
    if status is None:
        return True  # can't tell → keep the mount (fail safe)
    return status == 200


# cuids whose cleanup could not finish on the previous pass. A stuck orphan is retried
# every cycle, so it is logged only when it first gets stuck rather than every cycle.
_reap_pending = set()


def reap_once():
    global _reap_pending
    if not os.path.isdir(MOUNT_ROOT):
        return
    previously_pending = _reap_pending
    still_pending = set()

    for cuid in os.listdir(MOUNT_ROOT):
        cu_dir = os.path.join(MOUNT_ROOT, cuid)
        if not os.path.isdir(cu_dir) or _cu_pod_exists(cuid):
            continue
        first_attempt = cuid not in previously_pending
        if first_attempt:
            log(f"cu {cuid} pod is gone; unmounting {cu_dir}")

        for dirpath, _, _ in os.walk(cu_dir, topdown=False):
            if os.path.ismount(dirpath):
                # The mounter is root, so a plain lazy umount works (no setuid fusermount needed).
                result = subprocess.run(["umount", "-l", dirpath], capture_output=True, text=True)
                if result.returncode != 0 and first_attempt:
                    log(f"cu {cuid}: umount -l {dirpath} failed: {(result.stdout + result.stderr).strip()}")

        # A lazy umount only detaches once the last reference to the mount goes away, so a
        # mount another namespace still holds can outlive this pass. Removing the directory
        # would then fail on every cycle forever, so only remove it once nothing is mounted
        # underneath and leave it pending otherwise — the mounts are read-only, so an orphan
        # lingering for a few cycles is harmless.
        if any(os.path.ismount(dirpath) for dirpath, _, _ in os.walk(cu_dir)):
            if first_attempt:
                log(f"cu {cuid}: mounts still busy, leaving {cu_dir} for a later cycle")
            still_pending.add(cuid)
            continue

        shutil.rmtree(cu_dir, ignore_errors=True)
        if os.path.exists(cu_dir):
            if first_attempt:
                log(f"cu {cuid}: could not remove {cu_dir}, leaving it for a later cycle")
            still_pending.add(cuid)
        else:
            log(f"cu {cuid}: unmounted and removed {cu_dir}")

    _reap_pending = still_pending


def reaper_loop():
    while True:
        time.sleep(REAPER_INTERVAL_S)
        try:
            reap_once()
        except Exception as e:  # noqa: BLE001
            log(f"reaper error: {e}")


def main():
    ensure_shared_root()
    threading.Thread(target=reaper_loop, daemon=True).start()
    log(f"listening on :{MOUNTER_PORT}, mount root {MOUNT_ROOT}, pool ns {POOL_NAMESPACE}")
    ThreadingHTTPServer(("0.0.0.0", MOUNTER_PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
