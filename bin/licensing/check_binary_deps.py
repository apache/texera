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

"""Verify the prose in LICENSE-binary matches actually-bundled third-party
dependencies for one ecosystem (jar | npm | python).

Usage:
  check_binary_deps.py jar    <dist-lib-dir-1> [<dist-lib-dir-2> ...]
  check_binary_deps.py npm    <path-to-3rdpartylicenses.txt>
  check_binary_deps.py python <path-to-pip-licenses.csv>

Exits non-zero on drift; prints ADDED / STALE groups with remediation hints.

JVM format: each bullet is a per-jar distribution path of the form
`  - lib/<basename>.jar` and may carry an optional `(see licenses/...)`
pointer at the end. The check compares those exact filenames against the
basenames present in each dist's `lib/` directory — version drift is
intentionally surfaced. The check is purely "is each bundled jar claimed
somewhere in LICENSE-binary?" — it does not verify that a jar is in the
right license section. Reviewers police categorization manually.

npm / python format: each bullet names one package as its first token.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path


# Jars produced by Texera itself — not third-party deps, skip from drift checks.
TEXERA_OWN_JAR_PREFIX = "org.apache.texera."

ECO_HEADERS = {
    "jar":    "Scala/Java jars:",
    "python": "Python packages:",
    "npm":    "Angular / npm packages",
}

# `  - lib/<basename>.jar` optionally followed by ` (see licenses/...)`.
JAR_BULLET = re.compile(r"^\s*-\s+lib/(\S+\.jar)\b")
PKG_BULLET = re.compile(r"^\s*-\s+([@\w][\w@/.\-]*)")


# --- extracting claims from LICENSE-binary ---------------------------------

def parse_prose(path: Path, ecosystem: str) -> set[str]:
    """Return the set of claimed jar basenames (jar) or package names (npm/python)."""
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
        else:
            m = PKG_BULLET.match(raw)
            if m:
                name = m.group(1)
                if ecosystem == "python":
                    name = canonicalize_python_name(name)
                claims.add(name)

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
    """Angular CLI 3rdpartylicenses.txt: each entry is <name>\n<license>\n<text>."""
    result: set[str] = set()
    lines = path.read_text().splitlines()
    for i, line in enumerate(lines):
        if i == 0 or lines[i - 1].strip() == "":
            if (re.fullmatch(r"[@a-z][a-zA-Z0-9@/\._-]+", line)
                    and i + 1 < len(lines)
                    and re.match(r"^(MIT|BSD|Apache|ISC|[A-Z0-9.,()\- ]+)", lines[i + 1])):
                result.add(line)
    return result


def canonicalize_python_name(name: str) -> str:
    """PEP 503 canonical form: lowercase, [-_.]+ collapsed to '-'."""
    return re.sub(r"[-_.]+", "-", name.lower())


def collect_python(path: Path) -> set[str]:
    """pip-licenses CSV: Name,Version,License (header row). Names are
    canonicalized per PEP 503 so the compare is indifferent to whether
    a distribution uses hyphens, underscores, or dots."""
    result: set[str] = set()
    with path.open(newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            if row:
                result.add(canonicalize_python_name(row[0]))
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
    ap.add_argument("kind", choices=["jar", "npm", "python"])
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
