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
A background watcher unmounts a CU's directories as soon as its pod is deleted.
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
# How long the API server keeps a watch open. Each expiry re-runs the reconcile, which is
# the safety net for events missed while disconnected and for orphans still busy earlier.
WATCH_TIMEOUT_S = 300
WATCH_RETRY_S = 10

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
# Computing-unit pods are named "<prefix>-<cuid>"; the helm chart gives this and the
# computing-unit manager's kubernetes.compute-unit-pod-name-prefix the same value.
CU_POD_NAME_PREFIX = os.environ.get("CU_POD_NAME_PREFIX", "computing-unit")
PROC_MOUNTS = "/proc/mounts"


def log(msg):
    print(f"[mounter] {msg}", flush=True)


def _unescape(field):
    """Decode the four characters /proc/mounts escapes octally."""
    for code, char in (("\\040", " "), ("\\011", "\t"), ("\\012", "\n"), ("\\134", "\\")):
        field = field.replace(code, char)
    return field


def mount_targets_under(path):
    """Mount targets at or under `path`, deepest first.

    Read from /proc/mounts rather than using os.path.ismount(): when a FUSE server dies
    (the mounter being restarted kills every GeeseFS it started) its mount entry survives
    here, but stat()ing the mount point fails with ENOTCONN, which os.path.ismount()
    reports as "not a mount point". Such a dead mount would then never be unmounted, and
    its directory could never be removed.
    """
    path = os.path.normpath(path)
    prefix = path + "/"
    targets = []
    try:
        with open(PROC_MOUNTS) as mounts:
            for line in mounts:
                fields = line.split()
                if len(fields) < 2:
                    continue
                target = _unescape(fields[1])
                if target == path or target.startswith(prefix):
                    targets.append(target)
    except OSError as e:
        log(f"reading {PROC_MOUNTS} failed: {e}")
    # Deepest first, so nested mounts are detached before their parents.
    return sorted(targets, key=len, reverse=True)


def is_mounted(path):
    """True if `path` itself is a mount point, alive or dead."""
    return os.path.normpath(path) in mount_targets_under(path)


def _responds(path):
    """True if `path` can be stat()ed — false for a mount whose FUSE server is gone."""
    try:
        os.stat(path)
        return True
    except OSError:
        return False


def _remove_empty_dirs(path, cuid):
    """Remove `path` and any parents left empty, stopping at the CU's own directory."""
    stop_at = os.path.normpath(os.path.join(MOUNT_ROOT, cuid))
    path = os.path.normpath(path)
    while path.startswith(stop_at):
        try:
            os.rmdir(path)
        except OSError:
            return  # not empty (another commit is mounted here) or already gone
        path = os.path.dirname(path)


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
    if is_mounted(target):
        if _responds(target):
            log(f"{repo}:{commit} already mounted for cu {cuid} at {target}")
            return target
        # The GeeseFS process backing this mount is gone — most likely the mounter was
        # restarted, which kills every GeeseFS it started. The mount point survives but
        # every access to it fails with ENOTCONN, so detach it and mount again.
        log(f"{repo}:{commit} for cu {cuid} has a dead mount at {target}; remounting")
        subprocess.run(["umount", "-l", target], capture_output=True, text=True)

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
        # Nothing was mounted, so drop the directory just created for it rather than
        # leaving an empty one behind until the next resync. A rejected mount (an
        # unreadable repository, say) is a normal outcome, not a reason to litter.
        _remove_empty_dirs(target, cuid)
        raise RuntimeError(
            f"geesefs exited {result.returncode}: {(result.stdout + result.stderr).strip()}"
        )

    # GeeseFS daemonizes after a successful mount; wait until the kernel reports it.
    deadline = time.time() + MOUNT_TIMEOUT_S
    while not is_mounted(target):
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


# ---- watcher: unmount a CU's directories once its pod is deleted ----

def _k8s_open(path, timeout):
    """GET against the in-cluster API server. Returns the response; the caller must close it."""
    with open(os.path.join(SA_DIR, "token")) as f:
        token = f.read().strip()
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    port = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    ctx = ssl.create_default_context(cafile=os.path.join(SA_DIR, "ca.crt"))
    req = urllib.request.Request(
        f"https://{host}:{port}{path}", headers={"Authorization": f"Bearer {token}"}
    )
    return urllib.request.urlopen(req, timeout=timeout, context=ctx)


def _cuid_of(pod_name):
    """The cuid a CU pod name belongs to, or None if it is not a CU pod."""
    prefix = CU_POD_NAME_PREFIX + "-"
    if not pod_name.startswith(prefix):
        return None
    return pod_name[len(prefix):] or None


def _list_cu_pods():
    """(live cuids, resourceVersion) for the pool namespace, or (None, None) if unreachable."""
    try:
        with _k8s_open(f"/api/v1/namespaces/{POOL_NAMESPACE}/pods", timeout=30) as response:
            pods = json.loads(response.read())
    except Exception as e:  # noqa: BLE001
        log(f"listing CU pods failed: {e}")
        return None, None
    live = {
        cuid
        for cuid in (_cuid_of(p.get("metadata", {}).get("name", "")) for p in pods.get("items", []))
        if cuid
    }
    return live, pods.get("metadata", {}).get("resourceVersion")


def clean_cu_dir(cuid, quiet=False):
    """Unmount everything under a departed CU's directory and remove it.

    Returns True once the directory is gone. `quiet` suppresses the messages for an orphan
    already reported, so a repeatedly retried one does not re-log on every resync.
    """
    cu_dir = os.path.join(MOUNT_ROOT, cuid)
    # Never treat the shared root itself as a CU directory: unmounting it would break
    # propagation for every CU on this node.
    if os.path.normpath(cu_dir) == os.path.normpath(MOUNT_ROOT) or not os.path.isdir(cu_dir):
        return True
    if not quiet:
        log(f"cu {cuid} pod is gone; unmounting {cu_dir}")

    for target in mount_targets_under(cu_dir):
        # The mounter is root, so a plain lazy umount works (no setuid fusermount needed).
        result = subprocess.run(["umount", "-l", target], capture_output=True, text=True)
        if result.returncode != 0 and not quiet:
            log(f"cu {cuid}: umount -l {target} failed: {(result.stdout + result.stderr).strip()}")

    # A lazy umount only detaches once the last reference to the mount goes away, so a mount
    # another namespace still holds can outlive this call. Removing the directory would then
    # fail, so only remove it once nothing is mounted underneath and let the next resync retry
    # the rest — the mounts are read-only, so an orphan lingering a while is harmless.
    remaining = mount_targets_under(cu_dir)
    if remaining:
        if not quiet:
            log(f"cu {cuid}: {len(remaining)} mount(s) still busy, leaving {cu_dir} for the next resync")
        return False

    shutil.rmtree(cu_dir, ignore_errors=True)
    if os.path.exists(cu_dir):
        if not quiet:
            log(f"cu {cuid}: could not remove {cu_dir}, leaving it for the next resync")
        return False
    log(f"cu {cuid}: unmounted and removed {cu_dir}")
    return True


# cuids whose cleanup could not finish, retried on the next resync and logged only once.
_pending = set()


def reconcile(live_cuids):
    """Clean up every mount directory with no live CU pod behind it."""
    global _pending
    if not os.path.isdir(MOUNT_ROOT):
        return
    still_pending = set()
    for cuid in os.listdir(MOUNT_ROOT):
        if cuid in live_cuids or not os.path.isdir(os.path.join(MOUNT_ROOT, cuid)):
            continue
        if not clean_cu_dir(cuid, quiet=cuid in _pending):
            still_pending.add(cuid)
    _pending = still_pending


def _handle_event(line):
    try:
        event = json.loads(line)
    except ValueError:
        return
    if event.get("type") != "DELETED":
        return
    cuid = _cuid_of(event.get("object", {}).get("metadata", {}).get("name", ""))
    if cuid and not clean_cu_dir(cuid, quiet=cuid in _pending):
        _pending.add(cuid)


def watch_loop():
    """List-then-watch CU pods, unmounting a CU's mounts as soon as its pod is deleted.

    The initial LIST reconciles whatever was missed while the mounter was down; the WATCH
    then reacts to deletions immediately. The API server closes the watch every
    WATCH_TIMEOUT_S, and the resulting re-LIST doubles as the safety net for events missed
    across a disconnect and for orphans whose unmount could not complete earlier.
    """
    if not os.path.exists(os.path.join(SA_DIR, "token")):
        log("no service account token; not running in-cluster, pod watch disabled")
        return
    while True:
        live, resource_version = _list_cu_pods()
        if live is None:  # API unreachable → keep every mount (fail safe) and retry
            time.sleep(WATCH_RETRY_S)
            continue
        reconcile(live)
        try:
            with _k8s_open(
                f"/api/v1/namespaces/{POOL_NAMESPACE}/pods?watch=1"
                f"&resourceVersion={resource_version}&timeoutSeconds={WATCH_TIMEOUT_S}",
                timeout=WATCH_TIMEOUT_S + 30,
            ) as response:
                for line in response:
                    _handle_event(line)
        except Exception as e:  # noqa: BLE001
            log(f"pod watch failed ({e}); resyncing")
            time.sleep(WATCH_RETRY_S)


def main():
    ensure_shared_root()
    threading.Thread(target=watch_loop, daemon=True).start()
    log(f"listening on :{MOUNTER_PORT}, mount root {MOUNT_ROOT}, pool ns {POOL_NAMESPACE}")
    ThreadingHTTPServer(("0.0.0.0", MOUNTER_PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
