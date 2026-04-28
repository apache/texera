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

"""Diff actually-bundled deps against LICENSE-binary for one ecosystem
(jar | npm | agent-npm | python). Exits non-zero on drift.

Usage:
  check_binary_deps.py jar       <dist-lib-dir-1> [<dist-lib-dir-2> ...]
  check_binary_deps.py npm       <path-to-frontend-3rdpartylicenses.json>
  check_binary_deps.py agent-npm <path-to-agent-service-3rdpartylicenses.json>
  check_binary_deps.py python    <path-to-pip-licenses.csv>
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from pathlib import Path


# Jars produced by Texera itself — not third-party deps, skip from drift checks.
TEXERA_OWN_JAR_PREFIX = "org.apache.texera."

ECO_HEADERS = {
    "jar":       "Scala/Java jars:",
    "python":    "Python packages:",
    "npm":       "Angular / npm packages",
    "agent-npm": "Agent service npm packages",
}

JAR_BULLET = re.compile(r"^\s*-\s+(\S+\.jar)\b")
# `  - <name>@<version>` — npm form, name may start with @scope/.
NPM_BULLET = re.compile(r"^\s*-\s+(@?[\w@/.\-]+)@([^\s@]+)\s*$")
# `  - <name>==<version>` — pip form.
PY_BULLET  = re.compile(r"^\s*-\s+([\w][\w.\-]*)==(\S+)\s*$")


# --- extracting claims from LICENSE-binary ---------------------------------

def parse_prose(path: Path, ecosystem: str) -> set[str]:
    """Return the set of claimed entries:
       - jar:    set of jar basenames (e.g. 'commons-cli-1.5.0.jar' qualified)
       - npm:    set of '<name>@<version>'
       - python: set of '<canonical_name>==<version>'
    """
    lines = path.read_text().splitlines()
    current_eco: str | None = None
    claims: set[str] = set()

    for raw in lines:
        stripped = raw.strip()

        matched_header = False
        for eco, needle in ECO_HEADERS.items():
            if stripped.startswith(needle):
                current_eco = eco
                matched_header = True
                break
        if matched_header:
            continue

        if stripped.startswith("=====") or stripped.startswith("-----"):
            current_eco = None
            continue

        if current_eco != ecosystem:
            continue

        if ecosystem == "jar":
            m = JAR_BULLET.match(raw)
            if m:
                claims.add(m.group(1))
        elif ecosystem in ("npm", "agent-npm"):
            m = NPM_BULLET.match(raw)
            if m:
                claims.add(f"{m.group(1)}@{m.group(2)}")
        else:  # python
            m = PY_BULLET.match(raw)
            if m:
                name = canonicalize_python_name(m.group(1))
                ver  = canonicalize_python_version(m.group(2))
                claims.add(f"{name}=={ver}")

    return claims


# --- collecting reality ----------------------------------------------------

def collect_jars(lib_dirs) -> set[str]:
    result: set[str] = set()
    for d in lib_dirs:
        dp = Path(d)
        if not dp.is_dir():
            sys.stderr.write(f"error: {dp} is not a directory\n")
            sys.exit(2)
        for jar in dp.glob("*.jar"):
            if jar.name.startswith(TEXERA_OWN_JAR_PREFIX):
                continue
            result.add(jar.name)
    return result


def collect_npm(path: Path) -> set[str]:
    """3rdpartylicenses.json emitted by license-webpack-plugin (configured in
    frontend/custom-webpack.config.js): a JSON array of {name, version, license}
    entries scoped to the actual webpack bundle."""
    data = json.loads(path.read_text())
    return {f"{e['name']}@{e['version']}" for e in data if e.get('name') and e.get('version')}


def canonicalize_python_name(name: str) -> str:
    """PEP 503 canonical form: lowercase, [-_.]+ collapsed to '-'."""
    return re.sub(r"[-_.]+", "-", name.lower())


def canonicalize_python_version(version: str) -> str:
    """Drop PEP 440 local-version identifiers (everything after `+`).
    Wheels for the same release ship as e.g. `2.8.0` on macOS but
    `2.8.0+cpu` on Linux — same software, different platform tag."""
    return version.split("+", 1)[0]


def collect_python(path: Path) -> set[str]:
    """pip-licenses CSV: Name,Version,License (header row). Names are
    canonicalized per PEP 503 so the compare is indifferent to whether
    a distribution uses hyphens, underscores, or dots; versions are
    canonicalized to the public release form (no PEP 440 +local suffix)."""
    result: set[str] = set()
    with path.open(newline="") as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        for row in reader:
            if row and row[0] and row[1]:
                name = canonicalize_python_name(row[0])
                ver  = canonicalize_python_version(row[1])
                result.add(f"{name}=={ver}")
    return result


# --- matching & reporting --------------------------------------------------

def report(added: list[str], stale: list[str], label: str, kind: str) -> int:
    rc = 0
    if added:
        print(f"NEW {label} not claimed by LICENSE-binary:")
        for a in sorted(added):
            print(f"  + {a}")
        print()
        print("ACTION REQUIRED")
        print(f"  1. Verify each dep's license is ASF Category A or B.")
        print(f"  2. Add a bullet in LICENSE-binary under the matching license")
        print(f"     section, either as '{kind}-compatible token' (see format below).")
        print(f"  3. If an upstream NOTICE must be bubbled up, add to NOTICE-binary.")
        print()
        rc = 1

    if stale:
        print(f"STALE {label} claimed by LICENSE-binary but not actually bundled:")
        for s in sorted(stale):
            print(f"  - {s}")
        print()
        print("ACTION REQUIRED")
        print(f"  1. Remove the matching bullet / token from LICENSE-binary.")
        print(f"  2. Remove any matching attribution from NOTICE-binary.")
        print()
        rc = 1

    return rc


# --- main ------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("kind", choices=["jar", "npm", "agent-npm", "python"])
    ap.add_argument("inputs", nargs="+")
    ap.add_argument(
        "--license-binary",
        default=str(Path(__file__).resolve().parent.parent.parent / "LICENSE-binary"),
    )
    args = ap.parse_args()

    lb = Path(args.license_binary)
    if not lb.exists():
        sys.stderr.write(f"error: {lb} not found\n")
        return 2

    if args.kind == "jar":
        claimed = parse_prose(lb, "jar")
        reality = collect_jars(args.inputs)
        added = sorted(reality - claimed)
        stale = sorted(claimed - reality)
        rc = report(added, stale, "JVM jars", "jar")
        if rc == 0:
            print(f"OK: {len(reality)} JVM jars match LICENSE-binary.")
        return rc

    if args.kind == "npm":
        claimed = parse_prose(lb, "npm")
        reality = collect_npm(Path(args.inputs[0]))
        added = sorted(reality - claimed)
        stale = sorted(claimed - reality)
        rc = report(added, stale, "npm packages", "npm")
        if rc == 0:
            print(f"OK: {len(reality)} npm packages match LICENSE-binary.")
        return rc

    if args.kind == "agent-npm":
        claimed = parse_prose(lb, "agent-npm")
        reality = collect_npm(Path(args.inputs[0]))
        added = sorted(reality - claimed)
        stale = sorted(claimed - reality)
        rc = report(added, stale, "agent-service npm packages", "agent-npm")
        if rc == 0:
            print(f"OK: {len(reality)} agent-service npm packages match LICENSE-binary.")
        return rc

    if args.kind == "python":
        claimed = parse_prose(lb, "python")
        reality = collect_python(Path(args.inputs[0]))
        added = sorted(reality - claimed)
        stale = sorted(claimed - reality)
        rc = report(added, stale, "Python packages", "python")
        if rc == 0:
            print(f"OK: {len(reality)} Python packages match LICENSE-binary.")
        return rc

    return 2


if __name__ == "__main__":
    sys.exit(main())
