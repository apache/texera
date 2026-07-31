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

"""pytest fixtures for the texera-mounter tests.

The mounter is the entrypoint of its own image rather than a Python package, so we
load it with `importlib.util` the same way `bin/local-dev/tests` loads the TUI.

The module is loaded fresh per test because the tests mutate module-level state
(`MOUNT_ROOT`, `PROC_MOUNTS`, the pending-cleanup set), and it is pointed at a fake
mount root and a fake /proc/mounts under `tmp_path` so nothing touches the host. Test
helpers are attached to the module object so tests can stay terse: `mounter.logs`,
`mounter.runs`, `mounter.mounts()` and `mounter.set_mounts()`."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
MOUNTER_PATH = REPO_ROOT / "bin" / "mounter" / "mounter.py"


@pytest.fixture
def mounter(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("texera_mounter", MOUNTER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["texera_mounter"] = module
    spec.loader.exec_module(module)

    module.MOUNT_ROOT = str(tmp_path / "mounts")
    module.PROC_MOUNTS = str(tmp_path / "proc_mounts")
    os.makedirs(module.MOUNT_ROOT)

    module.logs = []
    module.log = module.logs.append

    def set_mounts(*targets):
        """Rewrite the fake /proc/mounts to hold exactly these FUSE mount targets."""
        with open(module.PROC_MOUNTS, "w") as mounts:
            # One unrelated entry, so tests also prove the filtering works.
            mounts.write("/dev/sda1 / ext4 rw,relatime 0 0\n")
            for target in targets:
                escaped = target.replace("\\", "\\134").replace(" ", "\\040")
                mounts.write(f"dataset-1 {escaped} fuse.geesefs ro,nosuid,allow_other 0 0\n")

    def mounts():
        """The FUSE targets currently in the fake /proc/mounts."""
        return module.mount_targets_under(module.MOUNT_ROOT)

    module.set_mounts = set_mounts
    module.mounts = mounts
    set_mounts()

    # Every subprocess call is recorded rather than run. `geesefs` and `umount` also
    # update the fake /proc/mounts, so the mount-table logic is exercised for real.
    module.runs = []
    module.umount_succeeds = True
    module.geesefs_returncode = 0
    # Set False to model a GeeseFS that exits 0 without the mount ever appearing.
    module.geesefs_mounts = True

    def fake_run(cmd, **kwargs):
        module.runs.append((list(cmd), kwargs))
        returncode = 0
        live = module.mount_targets_under(module.MOUNT_ROOT)
        if cmd[0] == "geesefs":
            returncode = module.geesefs_returncode
            if returncode == 0 and module.geesefs_mounts:
                set_mounts(*live, cmd[-1])
        elif cmd[0] == "umount":
            if module.umount_succeeds:
                set_mounts(*[t for t in live if t != os.path.normpath(cmd[-1])])
            else:
                returncode = 32
        return subprocess.CompletedProcess(cmd, returncode, stdout="", stderr="umount: target is busy")

    monkeypatch.setattr(module.subprocess, "run", fake_run)
    return module


@pytest.fixture
def cu_dir(mounter):
    """Create a CU's mount directory tree and return (cuid, cu_dir, mount target)."""

    def make(cuid, repo="dataset-1", commit="abc123"):
        target = os.path.join(mounter.MOUNT_ROOT, cuid, repo, commit)
        os.makedirs(target, exist_ok=True)
        return os.path.join(mounter.MOUNT_ROOT, cuid), target

    return make
