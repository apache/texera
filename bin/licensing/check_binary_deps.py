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

Strictness:
  Default (exact match): version drift on any package — direct or transitive —
  fails the run.
  --ignore-transitive-version: version drift on a *transitive* dep is
  reported as informational; presence of every claimed library and version
  agreement on every *direct* dep are still enforced.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

# Per-module LICENSE-binary files that the combined LICENSE-binary unions.
# Resolved relative to the repo root (parent of bin/licensing/).
PER_MODULE_LICENSE_BINARIES: list[str] = [
    "access-control-service/LICENSE-binary",
    "config-service/LICENSE-binary",
    "file-service/LICENSE-binary",
    "workflow-compiling-service/LICENSE-binary",
    "computing-unit-managing-service/LICENSE-binary",
    "amber/LICENSE-binary-java",
    "amber/LICENSE-binary-python",
    "frontend/LICENSE-binary",
    "agent-service/LICENSE-binary",
]

# Primary requirement files used to mark deps as "direct" (vs. transitive).
PRIMARY_REQUIREMENTS = {
    "python":    ["amber/requirements.txt", "amber/operator-requirements.txt"],
    "npm":       ["frontend/package.json"],
    "agent-npm": ["agent-service/package.json"],
    # All SBT files in the repo are scanned for libraryDependencies entries.
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def build_default_license_binary() -> Path:
    """Concat all per-module LICENSE-binary files into a temp file using
    bin/licensing/concat_license_binary.py and return its path. Used as
    the default --license-binary when the caller doesn't pass one."""
    here = Path(__file__).resolve().parent
    repo_root = _repo_root()
    inputs = [repo_root / p for p in PER_MODULE_LICENSE_BINARIES]
    missing = [p for p in inputs if not p.is_file()]
    if missing:
        sys.stderr.write(
            f"error: per-module LICENSE-binary file(s) not found: {missing}\n"
        )
        sys.exit(2)
    sys.path.insert(0, str(here))
    import concat_license_binary as concat
    parsed = [concat.parse(p) for p in inputs]
    apache_header, groups = concat.merge(parsed)
    text = concat.emit(apache_header, groups)
    out = Path(tempfile.mkstemp(prefix="combined-LICENSE-binary-", suffix=".txt")[1])
    out.write_text(text)
    return out


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

# Splits a jar basename like `netty-all-4.1.96.Final.jar` into (artifact, version).
# The version starts at the first dash followed by a digit.
JAR_NAME_VERSION = re.compile(r"^(.+?)-(\d[^/]*)\.jar$")

# SBT libraryDependencies syntax: "group" % "artifact" % "version" [...].
# %% / %%% mark Scala / Scala.js libs whose artifact gains a `_<scalaVer>` suffix
# at resolution time; we capture the bare artifact and reconstruct both forms.
SBT_DEP = re.compile(
    r'"([\w.\-]+)"\s*(%%%?|%)\s*"([\w.\-]+)"\s*%\s*"([\w.\-]+)"'
)


# --- direct-dep loaders ----------------------------------------------------

def load_direct_python() -> set[str]:
    """PEP 503 canonical names from amber/requirements.txt (and any other
    file listed in PRIMARY_REQUIREMENTS['python'])."""
    direct: set[str] = set()
    for rel in PRIMARY_REQUIREMENTS["python"]:
        p = _repo_root() / rel
        if not p.is_file():
            continue
        for raw in p.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith("-"):
                # `-`-prefixed lines are pip flags (--extra-index-url, -r, ...).
                continue
            # Strip env markers, extras, and version specifiers; we only
            # need the package name.
            name = re.split(r"[<>=!~;\s\[]", line, maxsplit=1)[0]
            if name:
                direct.add(canonicalize_python_name(name))
    return direct


def load_direct_npm(rel_path: str) -> set[str]:
    """Top-level dep names from a package.json's dependencies / devDependencies /
    peerDependencies / optionalDependencies."""
    direct: set[str] = set()
    p = _repo_root() / rel_path
    if not p.is_file():
        return direct
    data = json.loads(p.read_text())
    for key in ("dependencies", "devDependencies", "peerDependencies", "optionalDependencies"):
        section = data.get(key) or {}
        direct.update(section.keys())
    return direct


def load_direct_jar_artifacts() -> set[str]:
    """ArtifactIds declared in any *.sbt or project/Dependencies.scala file
    in the repo. For Scala libs (`%%`/`%%%`) the resolved jar gains a
    `_<scalaVer>` suffix; we add the bare artifact here and let the matcher
    strip the suffix when comparing."""
    repo_root = _repo_root()
    direct: set[str] = set()

    # Scan SBT build files. Walking the tree keeps this resilient to new
    # subprojects without having to keep an explicit list in sync.
    skip_dirs = {"node_modules", "target", ".git", ".idea", ".bsp", "dist"}
    for path in repo_root.rglob("*"):
        if any(part in skip_dirs for part in path.parts):
            continue
        if not path.is_file():
            continue
        if path.suffix == ".sbt" or path.name == "Dependencies.scala":
            try:
                text = path.read_text(errors="replace")
            except OSError:
                continue
            for m in SBT_DEP.finditer(text):
                _group, _sep, artifact, _version = m.groups()
                direct.add(artifact)
    return direct


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


# --- diff ------------------------------------------------------------------

# Trailing Scala-version suffix that SBT appends to %% artifacts at resolve time.
SCALA_SUFFIX = re.compile(r"_\d+(?:\.\d+)+$")


def _index_npm(items: set[str]) -> dict[str, set[str]]:
    """{ 'react@18.2.0', 'react@17.0.0' } -> { 'react': {'18.2.0', '17.0.0'} }.
    Same name can legitimately appear at multiple versions when the bundle
    pulls in two majors of a transitive dep."""
    out: dict[str, set[str]] = defaultdict(set)
    for entry in items:
        # Last '@' is the version separator; '@' inside scoped names is at index 0.
        idx = entry.rfind("@")
        if idx <= 0:
            continue
        out[entry[:idx]].add(entry[idx + 1:])
    return out


def _index_python(items: set[str]) -> dict[str, set[str]]:
    """{ 'numpy==2.1.0' } -> { 'numpy': {'2.1.0'} }."""
    out: dict[str, set[str]] = defaultdict(set)
    for entry in items:
        if "==" not in entry:
            continue
        name, _, ver = entry.partition("==")
        out[name].add(ver)
    return out


def _index_jar(items: set[str]) -> dict[str, set[str]]:
    """{ 'netty-all-4.1.96.Final.jar' } -> { 'netty-all': {'4.1.96.Final'} }.
    Same shape as _index_npm / _index_python: an artifact legitimately
    bundled at multiple versions (e.g. logback at 1.2.x in one service
    and 1.4.x in another) survives intact. Unparseable jar names are
    surfaced loudly rather than silently dropped — a parser bug here
    means real bundled deps would skip license validation."""
    out: dict[str, set[str]] = defaultdict(set)
    for jar in items:
        m = JAR_NAME_VERSION.match(jar)
        if not m:
            sys.stderr.write(f"warning: cannot parse jar name: {jar}\n")
            continue
        out[m.group(1)].add(m.group(2))
    return out


def _jar_basename(artifact: str, version: str) -> str:
    return f"{artifact}-{version}.jar"


def _is_direct_jar(artifact: str, direct_artifacts: set[str]) -> bool:
    """sbt-native-packager's default JavaAppPackaging names dist jars
    `<groupId>.<artifactId>-<version>.jar`, so the artifactId we extract from
    the basename is `<groupId>.<artifactId>` (e.g. `io.netty.netty-buffer`).
    SBT's libraryDependencies record only the bare artifactId, so we match
    on the segment after the last `.`. Scala libs (`%%`) get a `_<scalaVer>`
    suffix appended at resolve time which we strip first."""
    bare = SCALA_SUFFIX.sub("", artifact)
    if bare in direct_artifacts:
        return True
    if "." in bare:
        tail = bare.rsplit(".", 1)[1]
        # Re-strip in case the Scala suffix was after the dot.
        tail = SCALA_SUFFIX.sub("", tail)
        if tail in direct_artifacts:
            return True
    return False


# --- reporting -------------------------------------------------------------

def report(
    added: list[str],
    stale: list[str],
    drift_direct: list[tuple[str, str, str]],
    drift_transitive: list[tuple[str, str, str]],
    label: str,
    kind: str,
    ignore_transitive_version: bool,
) -> int:
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

    def _fmt_drift(entry: tuple[str, list[str], list[str]]) -> str:
        name, cvers, rvers = entry
        return f"  ~ {name}: LICENSE-binary={', '.join(cvers)}  bundled={', '.join(rvers)}"

    if drift_direct:
        print(f"DRIFT (direct) {label} — claimed versions differ from bundled:")
        for entry in sorted(drift_direct):
            print(_fmt_drift(entry))
        print()
        print("ACTION REQUIRED")
        print(f"  Update LICENSE-binary to match the bundled versions. Direct deps")
        print(f"  always block CI — a version bump may carry license changes.")
        print()
        rc = 1

    if drift_transitive:
        if ignore_transitive_version:
            print(f"DRIFT (transitive, informational) {label}:")
            for entry in sorted(drift_transitive):
                print(_fmt_drift(entry))
            print(f"  (--ignore-transitive-version is set; nightly exact-match")
            print(f"   check on main is responsible for refreshing these.)")
            print()
        else:
            print(f"DRIFT (transitive) {label} — claimed versions differ from bundled:")
            for entry in sorted(drift_transitive):
                print(_fmt_drift(entry))
            print()
            print("ACTION REQUIRED")
            print(f"  Update LICENSE-binary to match the bundled versions, or rerun")
            print(f"  with --ignore-transitive-version to treat transitive drift as")
            print(f"  informational.")
            print()
            rc = 1

    return rc


# --- main ------------------------------------------------------------------

def diff_simple(
    claim_idx: dict[str, set[str]],
    real_idx: dict[str, set[str]],
    direct_names: set[str],
    joiner: str,
) -> tuple[list[str], list[str], list[tuple[str, list[str], list[str]]], list[tuple[str, list[str], list[str]]]]:
    """Diff name->{versions} multimaps for npm/python. `joiner` is the
    separator used when rendering added/stale entries (`@` for npm, `==`
    for python). Drifts are returned as (name, sorted_claimed_versions,
    sorted_real_versions)."""
    added: list[str] = []
    stale: list[str] = []
    drift_direct: list[tuple[str, list[str], list[str]]] = []
    drift_transitive: list[tuple[str, list[str], list[str]]] = []

    for name in sorted(real_idx.keys() - claim_idx.keys()):
        for v in sorted(real_idx[name]):
            added.append(f"{name}{joiner}{v}")
    for name in sorted(claim_idx.keys() - real_idx.keys()):
        for v in sorted(claim_idx[name]):
            stale.append(f"{name}{joiner}{v}")
    for name in sorted(claim_idx.keys() & real_idx.keys()):
        cvers, rvers = claim_idx[name], real_idx[name]
        if cvers == rvers:
            continue
        entry = (name, sorted(cvers), sorted(rvers))
        (drift_direct if name in direct_names else drift_transitive).append(entry)
    return added, stale, drift_direct, drift_transitive


def diff_jars(
    claim_idx: dict[str, set[str]],
    real_idx: dict[str, set[str]],
    direct_artifacts: set[str],
) -> tuple[list[str], list[str], list[tuple[str, list[str], list[str]]], list[tuple[str, list[str], list[str]]]]:
    """Diff artifact->{versions} multimaps. Added/stale are rendered as
    full jar basenames users will see in LICENSE-binary; drifts are
    (artifact, sorted_claimed, sorted_real)."""
    added: list[str] = []
    stale: list[str] = []
    drift_direct: list[tuple[str, list[str], list[str]]] = []
    drift_transitive: list[tuple[str, list[str], list[str]]] = []

    for artifact in sorted(real_idx.keys() - claim_idx.keys()):
        for v in sorted(real_idx[artifact]):
            added.append(_jar_basename(artifact, v))
    for artifact in sorted(claim_idx.keys() - real_idx.keys()):
        for v in sorted(claim_idx[artifact]):
            stale.append(_jar_basename(artifact, v))
    for artifact in sorted(claim_idx.keys() & real_idx.keys()):
        cvers, rvers = claim_idx[artifact], real_idx[artifact]
        if cvers == rvers:
            continue
        entry = (artifact, sorted(cvers), sorted(rvers))
        if _is_direct_jar(artifact, direct_artifacts):
            drift_direct.append(entry)
        else:
            drift_transitive.append(entry)
    return added, stale, drift_direct, drift_transitive


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("kind", choices=["jar", "npm", "agent-npm", "python"])
    ap.add_argument("inputs", nargs="+")
    ap.add_argument(
        "--license-binary",
        default=None,
        help=(
            "Path to LICENSE-binary to validate against. If omitted, the "
            "tool builds a combined LICENSE-binary on the fly from the "
            "per-module files (see PER_MODULE_LICENSE_BINARIES)."
        ),
    )
    ap.add_argument(
        "--ignore-transitive-version",
        action="store_true",
        help=(
            "Treat version drift on transitive deps as informational instead "
            "of failing. Direct deps (declared in primary requirement files: "
            "build.sbt / package.json / requirements.txt) still block on any "
            "drift, and any added or removed package still blocks regardless "
            "of direct/transitive."
        ),
    )
    args = ap.parse_args()

    if args.license_binary is None:
        lb = build_default_license_binary()
    else:
        lb = Path(args.license_binary)
        if not lb.exists():
            sys.stderr.write(f"error: {lb} not found\n")
            return 2

    if args.kind == "jar":
        claimed = parse_prose(lb, "jar")
        reality = collect_jars(args.inputs)
        direct_artifacts = load_direct_jar_artifacts()
        added, stale, dd, dt = diff_jars(_index_jar(claimed), _index_jar(reality), direct_artifacts)
        rc = report(added, stale, dd, dt, "JVM jars", "jar", args.ignore_transitive_version)
        if rc == 0:
            print(f"OK: {len(reality)} JVM jars match LICENSE-binary.")
        return rc

    if args.kind == "npm":
        claimed = parse_prose(lb, "npm")
        reality = collect_npm(Path(args.inputs[0]))
        direct = load_direct_npm("frontend/package.json")
        added, stale, dd, dt = diff_simple(_index_npm(claimed), _index_npm(reality), direct, joiner="@")
        rc = report(added, stale, dd, dt, "npm packages", "npm", args.ignore_transitive_version)
        if rc == 0:
            print(f"OK: {len(reality)} npm packages match LICENSE-binary.")
        return rc

    if args.kind == "agent-npm":
        claimed = parse_prose(lb, "agent-npm")
        reality = collect_npm(Path(args.inputs[0]))
        direct = load_direct_npm("agent-service/package.json")
        added, stale, dd, dt = diff_simple(_index_npm(claimed), _index_npm(reality), direct, joiner="@")
        rc = report(added, stale, dd, dt, "agent-service npm packages", "agent-npm", args.ignore_transitive_version)
        if rc == 0:
            print(f"OK: {len(reality)} agent-service npm packages match LICENSE-binary.")
        return rc

    if args.kind == "python":
        claimed = parse_prose(lb, "python")
        reality = collect_python(Path(args.inputs[0]))
        direct = load_direct_python()
        added, stale, dd, dt = diff_simple(_index_python(claimed), _index_python(reality), direct, joiner="==")
        rc = report(added, stale, dd, dt, "Python packages", "python", args.ignore_transitive_version)
        if rc == 0:
            print(f"OK: {len(reality)} Python packages match LICENSE-binary.")
        return rc

    return 2


if __name__ == "__main__":
    sys.exit(main())
