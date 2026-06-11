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

"""Audit each bundled jar's META-INF/LICENSE* and META-INF/NOTICE* (plus
top-level LICENSE/NOTICE files like Lombok's), grouping by content hash
so intra-project duplicates collapse into a single row.

Surfaces jars whose upstream LICENSE preserves third-party copyrights
beyond the canonical SPDX template — those need a per-dep
licenses/LICENSE-<name>.txt under our convention from commit 6d2e17d4d.

Usage:
  audit_jar_licenses.py <dist-lib-dir-1> [<dist-lib-dir-2> ...]

Advisory only: prints a report and exits 0.
"""
from __future__ import annotations

import argparse
import hashlib
import re
import sys
import zipfile
from collections import defaultdict
from pathlib import Path


TEXERA_OWN_JAR_PREFIX = "org.apache.texera."

LICENSE_NAMES = {"license", "license.txt", "license.md", "licence", "licence.txt"}
NOTICE_NAMES  = {"notice",  "notice.txt",  "notice.md"}

COPYRIGHT_RE = re.compile(r"^\s*Copyright\b", re.IGNORECASE | re.MULTILINE)

# Canonical Apache-2.0 LICENSE shipped by Apache projects is ~11357 bytes.
# Anything materially larger (> 11600) is augmented with project-specific
# attributions or embedded third-party copyrights.
AUGMENTED_LICENSE_THRESHOLD = 11600


def _classify(parts: list[str]) -> str | None:
    """Return 'license', 'notice', or None for a zip entry path.

    Root level: only exact LICENSE/NOTICE basenames (Lombok-style).

    Anywhere under META-INF/: match by basename — 'license' or 'licence'
    in the name → license, 'notice' → notice. Picks up:
      META-INF/LICENSE, META-INF/LICENSE.txt
      META-INF/FastDoubleParser-LICENSE, META-INF/thirdparty-LICENSE
      META-INF/license/LICENSE.bouncycastle.txt        (netty-3.x)
      META-INF/licenses/com.ongres.scram/.../LICENSE   (postgresql JDBC)
    """
    if len(parts) == 1:
        base = parts[0].lower()
        if base in LICENSE_NAMES:
            return "license"
        if base in NOTICE_NAMES:
            return "notice"
        return None
    if parts[0].upper() != "META-INF":
        return None
    base = parts[-1].lower()
    if "license" in base or "licence" in base:
        return "license"
    if "notice" in base:
        return "notice"
    return None


def extract_license_notice(jar_path: Path) -> tuple[str, str] | None:
    """Concatenate every LICENSE/NOTICE-style file in a jar (root level,
    META-INF/, or META-INF/license[s]/). Returns (license, notice) text
    or None on bad zip."""
    licenses: list[str] = []
    notices:  list[str] = []
    try:
        with zipfile.ZipFile(jar_path) as zf:
            for name in zf.namelist():
                kind = _classify(name.split("/"))
                if kind is None:
                    continue
                try:
                    blob = zf.read(name).decode("utf-8", errors="replace")
                except Exception:
                    continue
                (licenses if kind == "license" else notices).append(blob)
    except zipfile.BadZipFile:
        return None
    return "\n".join(licenses), "\n".join(notices)


def short_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="replace")).hexdigest()[:10]


def collect_groups(lib_dirs: list[Path]) -> dict[tuple[str, str], list[tuple[str, str, str]]]:
    """Return {(license_hash, notice_hash): [(jar_name, license_text, notice_text), ...]}."""
    seen: dict[str, Path] = {}
    for d in lib_dirs:
        if not d.is_dir():
            sys.stderr.write(f"error: {d} is not a directory\n")
            sys.exit(2)
        for jar in d.glob("*.jar"):
            if jar.name.startswith(TEXERA_OWN_JAR_PREFIX):
                continue
            seen.setdefault(jar.name, jar)

    groups: dict[tuple[str, str], list[tuple[str, str, str]]] = defaultdict(list)
    for name, path in sorted(seen.items()):
        result = extract_license_notice(path)
        if result is None:
            continue
        lic, noti = result
        groups[(short_hash(lic), short_hash(noti))].append((name, lic, noti))
    return groups


def needs_per_dep_file(license_text: str, copyright_count: int) -> bool:
    """Strict criterion: ship a per-dep LICENSE file when the upstream LICENSE
    is materially larger than the canonical SPDX template, or contains an
    explicit "Licenses for included components"-style block, or carries 4+
    distinct Copyright attributions."""
    if len(license_text) > AUGMENTED_LICENSE_THRESHOLD:
        return True
    if "licenses for included components" in license_text.lower():
        return True
    if copyright_count >= 4:
        return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("lib_dirs", nargs="+", help="dist lib/ directories to audit")
    args = ap.parse_args()

    groups = collect_groups([Path(d) for d in args.lib_dirs])

    rows = []
    for (lic_h, not_h), members in groups.items():
        sample_lic = members[0][1]
        sample_not = members[0][2]
        cprights = len(COPYRIGHT_RE.findall(sample_lic))
        rows.append({
            "lic_hash": lic_h,
            "not_hash": not_h,
            "lic_size": len(sample_lic),
            "not_size": len(sample_not),
            "cprights": cprights,
            "needs":    needs_per_dep_file(sample_lic, cprights),
            "members":  [m[0] for m in members],
        })

    rows.sort(key=lambda r: (-int(r["needs"]), -r["cprights"], -r["lic_size"]))
    needs = [r for r in rows if r["needs"]]
    flagged_other = [r for r in rows if not r["needs"] and (r["cprights"] > 0 or r["not_size"] > 0)]

    total_jars = sum(len(r["members"]) for r in rows)
    print(f"Audited {total_jars} jars across {len(rows)} distinct (license, notice) groups.")
    print(f"Groups warranting a per-dep licenses/LICENSE-*.txt file: {len(needs)}")
    print(f"Other flagged groups (project-attribution only, covered by shared license): {len(flagged_other)}")
    print()

    print("=" * 110)
    print("GROUPS REQUIRING A PER-DEP LICENSE FILE")
    print("=" * 110)
    print(f"{'lic_hash':10}  {'cp':>3}  {'lic_sz':>7}  {'not_sz':>7}  {'jars':>4}  members")
    print("-" * 110)
    for r in needs:
        head = ", ".join(r["members"][:3])
        more = f" (+{len(r['members']) - 3} more)" if len(r["members"]) > 3 else ""
        print(f"{r['lic_hash']:10}  {r['cprights']:>3}  {r['lic_size']:>7}  {r['not_size']:>7}  {len(r['members']):>4}  {head}{more}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
