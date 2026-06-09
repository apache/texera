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

"""Verify that requirements.txt and system-requirements-lock.txt stay in sync.

For every pinned package in requirements.txt, the lock file must contain the
same package pinned to the same version. The lock file is a superset: it also
pins transitive dependencies, so packages that appear only in the lock file are
allowed. The script exits non-zero and prints a report when a requirement is
missing from the lock file or pinned to a different version.

Usage:
    python .github/scripts/check_requirements_lock_sync.py [requirements] [lock]

Both paths are optional and default to amber/requirements.txt and
amber/system-requirements-lock.txt.
"""

import re
import sys
from pathlib import Path

DEFAULT_REQUIREMENTS = "amber/requirements.txt"
DEFAULT_LOCK = "amber/system-requirements-lock.txt"


def normalize_name(name: str) -> str:
    """Normalize a package name per PEP 503 so e.g. ``typing_extensions`` and
    ``typing-extensions`` compare equal."""
    return re.sub(r"[-_.]+", "-", name).lower()


def parse_pinned(path: str) -> dict[str, tuple[str, str]]:
    """Parse ``name==version`` lines from a requirements/lock file.

    Returns a mapping of normalized name -> (original name, version). Comment
    and blank lines are ignored, as are any lines that are not exact pins.
    """
    packages: dict[str, tuple[str, str]] = {}
    for raw_line in Path(path).read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "==" not in line:
            continue
        name, version = line.split("==", 1)
        packages[normalize_name(name)] = (name.strip(), version.strip())
    return packages


def find_sync_errors(requirements: dict, lock: dict) -> tuple[list[str], list[str]]:
    """Return (missing, mismatched) lists describing how requirements drifts from lock."""
    missing: list[str] = []
    mismatched: list[str] = []
    for key, (name, version) in sorted(requirements.items()):
        if key not in lock:
            missing.append(f"{name}=={version}")
        elif lock[key][1] != version:
            mismatched.append(f"{name}: requirements pins {version}, lock pins {lock[key][1]}")
    return missing, mismatched


def main(argv: list[str]) -> int:
    req_path = argv[1] if len(argv) > 1 else DEFAULT_REQUIREMENTS
    lock_path = argv[2] if len(argv) > 2 else DEFAULT_LOCK

    requirements = parse_pinned(req_path)
    lock = parse_pinned(lock_path)

    missing, mismatched = find_sync_errors(requirements, lock)

    if not missing and not mismatched:
        print(
            f"OK: all {len(requirements)} packages in {req_path} are present "
            f"in {lock_path} with matching versions."
        )
        return 0

    print(f"ERROR: {req_path} and {lock_path} are out of sync.")
    if missing:
        print("\nMissing from the lock file:")
        for entry in missing:
            print(f"  - {entry}")
    if mismatched:
        print("\nVersion mismatches:")
        for entry in mismatched:
            print(f"  - {entry}")
    print(
        f"\nRegenerate {lock_path} so every package in {req_path} is present "
        "at the same version."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
