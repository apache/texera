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

JVM format: each bullet contains one or more comma-separated tokens inside
parentheses. A token is either:
  - the full version-stripped jar basename, e.g. `org.apache.arrow.arrow-vector`
  - a prefix wildcard ending in `.*`, e.g. `org.apache.pekko.*`
The check is purely "is each bundled library claimed somewhere in
LICENSE-binary?" — it does not verify that a library is in the right
license section. Reviewers police categorization manually.

npm / python format: each bullet names one package as its first token.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path


SCALA_SENTINEL = "\x01SCALADOT\x01"

ECO_HEADERS = {
    "jar":    "Scala/Java jars:",
    "python": "Python packages:",
    "npm":    "Angular / npm packages",
}


# --- extracting claims from LICENSE-binary ---------------------------------

def parse_prose(path: Path, ecosystem: str):
    """Return (exact_set, wildcard_prefixes) for jars; a set of pkg names for npm/python."""
    lines = path.read_text().splitlines()
    current_eco: str | None = None
    buffer = ""
    exact: set[str] = set()
    wildcards: set[str] = set()
    pkgs: set[str] = set()

    def flush():
        nonlocal buffer
        if not buffer or current_eco != ecosystem:
            buffer = ""
            return
        if ecosystem == "jar":
            # Tokens inside parentheticals.
            for m in re.finditer(r"\(([^)]+)\)", buffer):
                for tok in m.group(1).split(","):
                    tok = tok.strip()
                    if not tok:
                        continue
                    if tok.endswith(".*"):
                        wildcards.add(tok[:-2])
                    else:
                        exact.add(tok)
        else:
            m = re.match(r"\s*-\s+([@\w][\w@/.\-]*)", buffer)
            if m:
                pkgs.add(m.group(1))
        buffer = ""

    for raw in lines:
        stripped = raw.strip()

        # Ecosystem sub-header?
        matched_header = False
        for eco, needle in ECO_HEADERS.items():
            if stripped.startswith(needle):
                flush()
                current_eco = eco
                matched_header = True
                break
        if matched_header:
            continue

        if stripped.startswith("=====") or stripped.startswith("-----"):
            flush()
            current_eco = None
            continue

        if re.match(r"^  - ", raw):
            flush()
            buffer = raw
        elif re.match(r"^    \S", raw) and buffer:
            buffer += " " + stripped
        else:
            flush()

    flush()

    if ecosystem == "jar":
        return exact, wildcards
    return pkgs


# --- collecting reality ----------------------------------------------------

def strip_version(stem: str) -> str:
    protected = re.sub(r"_([23])\.([0-9]+)", rf"_\1{SCALA_SENTINEL}\2", stem)
    protected = re.sub(r"-[0-9].*$", "", protected)
    return protected.replace(SCALA_SENTINEL, ".")


def collect_jars(lib_dirs) -> set[str]:
    result: set[str] = set()
    for d in lib_dirs:
        dp = Path(d)
        if not dp.is_dir():
            sys.stderr.write(f"error: {dp} is not a directory\n")
            sys.exit(2)
        for jar in dp.glob("*.jar"):
            result.add(strip_version(jar.stem))
    return result


def collect_npm(path: Path) -> set[str]:
    """Angular CLI 3rdpartylicenses.txt: each entry is <name>\n<license>\n<text>."""
    result: set[str] = set()
    lines = path.read_text().splitlines()
    prev = ""
    for line in lines:
        if re.fullmatch(r"[@a-z][a-zA-Z0-9@/\._-]+", line) and prev == "":
            # Peek at the next line — should be a license identifier.
            idx = lines.index(line, lines.index(line))
            # Simpler: check the following line via enumerate.
            pass
        prev = line

    # Cleaner: iterate with index.
    result.clear()
    for i, line in enumerate(lines):
        if i == 0 or lines[i - 1].strip() == "":
            if (re.fullmatch(r"[@a-z][a-zA-Z0-9@/\._-]+", line)
                    and i + 1 < len(lines)
                    and re.match(r"^(MIT|BSD|Apache|ISC|[A-Z0-9.,()\- ]+)", lines[i + 1])):
                result.add(line)
    return result


def collect_python(path: Path) -> set[str]:
    """pip-licenses CSV: Name,Version,License (header row)."""
    result: set[str] = set()
    with path.open(newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        for row in reader:
            if row:
                result.add(row[0].lower())
    return result


# --- matching & reporting --------------------------------------------------

def match_jar(jar: str, exact: set[str], wildcards: set[str]) -> bool:
    if jar in exact:
        return True
    for w in wildcards:
        if jar == w or jar.startswith(w + "."):
            return True
    return False


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
        exact, wildcards = parse_prose(lb, "jar")
        reality = collect_jars(args.inputs)

        added = [j for j in reality if not match_jar(j, exact, wildcards)]
        claimed_exact_hit = {j for j in reality if j in exact}
        claimed_wildcard_hit = {j for j in reality if any(
            j == w or j.startswith(w + ".") for w in wildcards)}

        stale_exact = [e for e in exact if e not in reality]
        stale_wildcards = [w for w in wildcards
                           if not any(j == w or j.startswith(w + ".") for j in reality)]

        stale = stale_exact + [f"{w}:*" for w in stale_wildcards]
        rc = report(added, stale, "JVM jars", "jar")
        if rc == 0:
            print(f"OK: {len(reality)} JVM jars match LICENSE-binary "
                  f"({len(claimed_exact_hit)} explicit + "
                  f"{len(claimed_wildcard_hit) - len(claimed_exact_hit & claimed_wildcard_hit)} wildcard).")
        return rc

    if args.kind == "npm":
        pkgs = parse_prose(lb, "npm")
        reality = collect_npm(Path(args.inputs[0]))
        added = sorted(reality - pkgs)
        stale = sorted(pkgs - reality)
        rc = report(added, stale, "npm packages", "npm")
        if rc == 0:
            print(f"OK: {len(reality)} npm packages match LICENSE-binary.")
        return rc

    if args.kind == "python":
        pkgs = parse_prose(lb, "python")
        reality = collect_python(Path(args.inputs[0]))
        added = sorted(reality - pkgs)
        stale = sorted(pkgs - reality)
        rc = report(added, stale, "Python packages", "python")
        if rc == 0:
            print(f"OK: {len(reality)} Python packages match LICENSE-binary.")
        return rc

    return 2


if __name__ == "__main__":
    sys.exit(main())
