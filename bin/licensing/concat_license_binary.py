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

"""Merge multiple per-module LICENSE-binary files into a single combined
LICENSE-binary, joining at the license-group level so that each ecosystem
subsection (Scala/Java jars / Python packages / Angular npm / Agent npm /
Source files derived) appears under its parent license group rather than
the inputs being stacked end-to-end.

Each per-module input file follows this structure:

    <Apache-2.0 license text>
    ===
    THIRD-PARTY COMPONENTS
    ===
    <module preamble>
    --- Dependencies under the X License ---
    [optional sub-license heading like "CDDL 1.0\\n~~~~"]
    Scala/Java jars: | Python packages: | ... :
      - <entry>
    --- Dependencies under the Y License ---
    ...
    Individual jars may contain their own META-INF/LICENSE...

The merge:
  - Reuses the Apache-2.0 license text from the first input verbatim.
  - Builds one canonical THIRD-PARTY COMPONENTS preamble.
  - Walks license groups in first-seen order across inputs (later-only
    groups append after earlier ones).
  - Within each group, unions subsections by header, ordered by
    SUBSECTION_ORDER.
  - Within each subsection, unions entries (deduplicating by entry id —
    the trimmed text after the leading "- ").
  - Emits the trailer once at the end.

Usage:
  concat_license_binary.py <output> <input1> [<input2> ...]
"""
from __future__ import annotations

import argparse
import sys
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path


SEP = "-" * 80
EQ = "=" * 80

# Fixed display order of subsections inside any license group.
SUBSECTION_ORDER = [
    "Source files derived",          # prefix; full headers vary by license name
    "Scala/Java jars:",
    "Python packages:",
    "Angular / npm packages:",
    "Agent service npm packages:",
]

PREAMBLE = (
    "Apache Texera's binary distribution bundles the following third-party\n"
    "components, grouped by license. Each section references licenses/ for\n"
    "the full text of the applicable license. Components under the Apache\n"
    "License, Version 2.0 are governed by the same license terms as Apache\n"
    "Texera itself and are listed for completeness."
)

TRAILER = (
    "Individual jars may contain their own META-INF/LICENSE and META-INF/NOTICE\n"
    "files that apply to their specific contents; those files continue to govern\n"
    "the use of those components."
)


@dataclass
class Subsection:
    header: str
    sub_license: str | None = None  # e.g. "CDDL 1.0\n~~~~~~~~", or None
    entries: list[list[str]] = field(default_factory=list)


@dataclass
class Group:
    header_block: list[str]
    title: str
    # Keyed by (sub_license, header). Two subsections with the same header
    # (e.g. "Scala/Java jars:") under different sub-licenses (CDDL 1.0 vs
    # CDDL 1.1) are distinct entries — that's why we key on the tuple.
    subsections: "OrderedDict[tuple[str | None, str], Subsection]" = field(default_factory=OrderedDict)

    def has_entries(self) -> bool:
        return any(s.entries for s in self.subsections.values())


def is_subsection_header(line: str) -> bool:
    s = line.rstrip()
    if not s.endswith(":"):
        return False
    return any(s == h or s.startswith(p) for p, h in
               ((p, p) for p in SUBSECTION_ORDER))


def entry_id(entry: list[str]) -> str:
    """Identifier used for deduplication: the trimmed text after the
    leading '  - '. For multi-line entries (Source files derived), the
    first line uniquely identifies the entry."""
    return entry[0][4:].strip()


def parse(path: Path) -> tuple[str, list[Group]]:
    """Parse a per-module LICENSE-binary into (apache_header, groups)."""
    text = path.read_text()
    lines = text.splitlines()

    third_party_idx = next(
        i for i, line in enumerate(lines) if "THIRD-PARTY COMPONENTS" in line
    )
    eq_idx = third_party_idx - 1
    while eq_idx >= 0 and not lines[eq_idx].startswith("="):
        eq_idx -= 1
    apache_header = "\n".join(lines[:eq_idx]).rstrip("\n")

    # Skip past THIRD-PARTY COMPONENTS preamble until first group SEP.
    i = eq_idx
    while i < len(lines) and lines[i] != SEP:
        i += 1

    groups: list[Group] = []
    while i < len(lines):
        if lines[i] != SEP:
            i += 1
            continue
        # Group header: SEP / title (one or more lines) / SEP
        header_block = [lines[i]]
        i += 1
        title_lines: list[str] = []
        while i < len(lines) and lines[i] != SEP:
            title_lines.append(lines[i])
            i += 1
        title = " ".join(s.strip() for s in title_lines).strip()
        header_block.extend(title_lines)
        if i < len(lines):
            header_block.append(lines[i])  # closing SEP
            i += 1

        grp = Group(header_block=header_block, title=title)
        current: Subsection | None = None
        current_sub_license: str | None = None
        while i < len(lines) and lines[i] != SEP:
            line = lines[i]
            # Sub-license heading (e.g. "CDDL 1.0\n~~~~~~~~"). A group can
            # carry multiple of these (CDDL 1.0 then CDDL 1.1); each acts
            # as a scope marker for the subsections that follow.
            if i + 1 < len(lines) and lines[i + 1].startswith("~~~"):
                if current is not None:
                    grp.subsections[(current.sub_license, current.header)] = current
                    current = None
                current_sub_license = line + "\n" + lines[i + 1]
                i += 3
                continue
            # Subsection header (ends with ':'; matches our known set)
            stripped = line.rstrip()
            if stripped.endswith(":") and any(
                stripped == hdr or stripped.startswith(prefix)
                for prefix, hdr in
                [(p, p) for p in SUBSECTION_ORDER]
            ):
                if current is not None:
                    grp.subsections[(current.sub_license, current.header)] = current
                current = Subsection(header=stripped, sub_license=current_sub_license)
                i += 1
                continue
            # Entry: "  - <id>"; continuation lines start with 4 spaces and
            # do NOT start with "  - " (those would be the next entry).
            if current is not None and line.startswith("  - "):
                entry = [line]
                i += 1
                while i < len(lines):
                    nxt = lines[i]
                    if nxt.startswith("    ") and not nxt.startswith("  - "):
                        entry.append(nxt)
                        i += 1
                    else:
                        break
                current.entries.append(entry)
                continue
            i += 1
        if current is not None:
            grp.subsections[(current.sub_license, current.header)] = current
        groups.append(grp)

    return apache_header, groups


def merge(parsed: list[tuple[str, list[Group]]]) -> tuple[str, list[Group]]:
    apache_header = parsed[0][0]
    merged: "OrderedDict[str, Group]" = OrderedDict()
    for _, groups in parsed:
        for g in groups:
            if g.title not in merged:
                merged[g.title] = Group(
                    header_block=list(g.header_block),
                    title=g.title,
                )
            mg = merged[g.title]
            for key, sub in g.subsections.items():
                if key not in mg.subsections:
                    mg.subsections[key] = Subsection(
                        header=sub.header,
                        sub_license=sub.sub_license,
                    )
                target = mg.subsections[key]
                seen = {entry_id(e) for e in target.entries}
                for e in sub.entries:
                    eid = entry_id(e)
                    if eid not in seen:
                        target.entries.append(e)
                        seen.add(eid)

    # Reorder subsections within each group: group by sub_license bucket
    # (preserving first-seen sub-license order), and within each bucket
    # order by SUBSECTION_ORDER.
    for mg in merged.values():
        sub_license_order: list[str | None] = []
        by_sub_license: "OrderedDict[str | None, list[tuple[tuple[str | None, str], Subsection]]]" = OrderedDict()
        for key, sub in mg.subsections.items():
            sl = sub.sub_license
            if sl not in by_sub_license:
                by_sub_license[sl] = []
                sub_license_order.append(sl)
            by_sub_license[sl].append((key, sub))

        ordered: "OrderedDict[tuple[str | None, str], Subsection]" = OrderedDict()
        for sl in sub_license_order:
            bucket = by_sub_license[sl]
            placed: set[tuple[str | None, str]] = set()
            for prefix in SUBSECTION_ORDER:
                for key, sub in bucket:
                    if sub.header.startswith(prefix) and key not in placed:
                        ordered[key] = sub
                        placed.add(key)
            for key, sub in bucket:
                if key not in placed:
                    ordered[key] = sub
        mg.subsections = ordered

    return apache_header, list(merged.values())


def emit(apache_header: str, groups: list[Group]) -> str:
    out: list[str] = [
        apache_header,
        "",
        EQ,
        "THIRD-PARTY COMPONENTS",
        EQ,
        "",
        PREAMBLE,
        "",
    ]
    for g in groups:
        if not g.has_entries():
            continue
        out.extend(g.header_block)
        out.append("")
        last_sub_license: str | None = None
        last_sub_license_emitted = False
        for sub in g.subsections.values():
            if not sub.entries:
                continue
            # Emit sub-license heading once whenever the marker changes.
            if sub.sub_license != last_sub_license or not last_sub_license_emitted:
                if sub.sub_license:
                    out.append(sub.sub_license)
                    out.append("")
                last_sub_license = sub.sub_license
                last_sub_license_emitted = True
            out.append(sub.header)
            for entry in sub.entries:
                out.extend(entry)
            out.append("")
    out.append(TRAILER)
    out.append("")
    return "\n".join(out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("output", help="Path to write the combined LICENSE-binary")
    ap.add_argument("inputs", nargs="+", help="Per-module LICENSE-binary files to merge")
    args = ap.parse_args()

    parsed = []
    for p in args.inputs:
        path = Path(p)
        if not path.is_file():
            sys.stderr.write(f"error: {path} is not a file\n")
            return 2
        parsed.append(parse(path))

    apache_header, groups = merge(parsed)
    text = emit(apache_header, groups)
    Path(args.output).write_text(text)

    n_entries = sum(len(s.entries) for g in groups for s in g.subsections.values())
    print(f"Wrote {args.output}: {len(groups)} groups, {n_entries} entries "
          f"from {len(args.inputs)} input file(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
