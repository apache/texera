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

"""Generate a NOTICE-binary for a service from its bundled jars' META-INF/NOTICE
files.

The output starts with the project's own NOTICE (Texera ASF header), then
emits one block per unique META-INF/NOTICE content (deduped by SHA-1 hash
across the jars in the given lib dirs). Each block is headed by a synthesized
project name derived from the longest common prefix of its members' Maven
coordinates, plus the list of contributing jars.

Blocks are sorted by jar count (largest cluster first), with hash as a stable
tiebreaker.

Optional `--extras <file>` appends a verbatim text file at the end. Use this
for non-jar attributions (Apache-2.0 Python wheels like aiohttp, Matplotlib)
that don't ship a NOTICE inside any jar.

Usage:
  generate_notice_binary.py <output> <lib-dir-1> [<lib-dir-2> ...] [--extras <file>] [--project-notice <NOTICE>]
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import zipfile
from collections import defaultdict
from pathlib import Path


SEP = "-" * 80
TEXERA_OWN_JAR_PREFIX = "org.apache.texera."

NOTICE_NAMES_TOPLEVEL = {"notice", "notice.txt", "notice.md"}


def is_notice_entry(parts: list[str]) -> bool:
    """Return True if the zip entry path is a NOTICE-style file we want to
    pick up. Mirrors audit_jar_licenses.py's classifier (notice side)."""
    if len(parts) == 1:
        return parts[0].lower() in NOTICE_NAMES_TOPLEVEL
    if parts[0].upper() != "META-INF":
        return False
    return "notice" in parts[-1].lower()


def extract_notice_blob(jar_path: Path) -> str | None:
    """Concatenate every NOTICE-style file in a jar (root level or
    META-INF/...) into one blob. Return None for bad-zip jars or jars
    whose NOTICE blobs are all empty."""
    pieces: list[str] = []
    try:
        with zipfile.ZipFile(jar_path) as zf:
            for name in zf.namelist():
                if is_notice_entry(name.split("/")):
                    try:
                        raw = zf.read(name).decode("utf-8", errors="replace")
                    except Exception:
                        continue
                    # Normalize line endings: jars from Windows-built upstreams
                    # ship CRLF, which git auto-converts on commit and would
                    # cause spurious drift between committed and regenerated.
                    blob = raw.replace("\r\n", "\n").replace("\r", "\n").strip()
                    if blob:
                        pieces.append(blob)
    except zipfile.BadZipFile:
        return None
    if not pieces:
        return None
    return "\n\n".join(pieces)


def short_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="replace")).hexdigest()[:10]


def project_name_for_cluster(jar_names: list[str]) -> str:
    """Return a heading for a cluster of jars sharing a NOTICE.

    Uses the longest common dotted prefix of the jar filenames (e.g.
    `org.apache.hadoop.hadoop-annotations-3.3.3.jar` and siblings yield
    `org.apache.hadoop`). For single-jar clusters, returns the jar name
    without `.jar`. The "Bundled jars: ..." line in each block lists
    exact filenames, so the heading just needs to be a navigational
    summary, not a polished project label.
    """
    if not jar_names:
        return "(unknown)"
    if len(jar_names) == 1:
        name = jar_names[0]
        return name[:-4] if name.endswith(".jar") else name
    common = os.path.commonprefix(jar_names)
    if "." in common:
        common = common[: common.rfind(".")]
    return common or jar_names[0]


def collect_clusters(lib_dirs: list[Path]) -> dict[str, dict]:
    """Return {hash: {'content': str, 'jars': sorted list[str]}} for every
    unique NOTICE blob found across the union of lib dirs."""
    seen_jars: dict[str, Path] = {}
    for d in lib_dirs:
        if not d.is_dir():
            sys.stderr.write(f"error: {d} is not a directory\n")
            sys.exit(2)
        for jar in d.glob("*.jar"):
            if jar.name.startswith(TEXERA_OWN_JAR_PREFIX):
                continue
            seen_jars.setdefault(jar.name, jar)

    clusters: dict[str, dict] = defaultdict(lambda: {"content": "", "jars": []})
    for name, path in sorted(seen_jars.items()):
        blob = extract_notice_blob(path)
        if not blob:
            continue
        h = short_hash(blob)
        clusters[h]["content"] = blob
        clusters[h]["jars"].append(name)
    for c in clusters.values():
        c["jars"].sort()
    return clusters


def emit_block(heading: str, jars: list[str], content: str) -> str:
    out: list[str] = []
    out.append(SEP)
    out.append(heading)
    out.append(SEP)
    out.append("")
    if len(jars) <= 4:
        out.append("Bundled jars: " + ", ".join(jars))
    else:
        out.append(f"Bundled jars ({len(jars)}): " + ", ".join(jars[:3]) + f", ... (+{len(jars) - 3} more)")
    out.append("")
    out.append(content)
    out.append("")
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("output", help="Path to write the NOTICE-binary")
    ap.add_argument("lib_dirs", nargs="+", help="lib/ directories to scan for jars")
    ap.add_argument(
        "--project-notice",
        default=str(Path(__file__).resolve().parent.parent.parent / "NOTICE"),
        help="Path to project's own NOTICE file (Texera ASF header). "
             "Prepended verbatim to the output.",
    )
    ap.add_argument(
        "--extras",
        default=None,
        help="Optional path to a file containing additional NOTICE blocks "
             "(e.g. for non-jar Apache-2.0 deps like aiohttp / matplotlib). "
             "Appended verbatim to the output.",
    )
    args = ap.parse_args()

    project_notice = Path(args.project_notice)
    if not project_notice.is_file():
        sys.stderr.write(f"error: --project-notice {project_notice} not found\n")
        return 2

    clusters = collect_clusters([Path(d) for d in args.lib_dirs])

    parts: list[str] = []
    parts.append(project_notice.read_text().rstrip())
    parts.append("")
    # Sort: by jar count desc, then by hash for stable ordering.
    sorted_clusters = sorted(
        clusters.items(),
        key=lambda kv: (-len(kv[1]["jars"]), kv[0]),
    )
    for h, c in sorted_clusters:
        heading = project_name_for_cluster(c["jars"])
        parts.append(emit_block(heading, c["jars"], c["content"]))

    if args.extras:
        extras = Path(args.extras)
        if not extras.is_file():
            sys.stderr.write(f"error: --extras {extras} not found\n")
            return 2
        parts.append(extras.read_text().rstrip())
        parts.append("")

    Path(args.output).write_text("\n".join(parts))
    print(
        f"Wrote {args.output}: {len(clusters)} unique NOTICE blocks "
        f"from {sum(len(c['jars']) for c in clusters.values())} jars across "
        f"{len(args.lib_dirs)} lib dir(s)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
