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
Collect licensing info from jars bundled in a Texera binary distribution.

Given a directory that contains bundled jars (e.g. the lib/ directory of a
sbt-native-packager dist zip, or /texera/lib or /texera/amber/lib from an
extracted Docker image), this script extracts each jar's META-INF licensing
artefacts into a staging directory and produces:

  - <out>/bundled/<jar-basename>/META-INF-LICENSE      (if present in jar)
  - <out>/bundled/<jar-basename>/META-INF-NOTICE       (if present in jar)
  - <out>/bundled/<jar-basename>/pom.xml               (if present in jar)
  - <out>/NOTICE-binary-skeleton                       (concatenated NOTICE files)
  - <out>/summary.tsv                                  (per-jar overview)

The output is a skeleton to help human reviewers assemble / update
LICENSE-binary. It is NOT a substitute for manual review: POM license
declarations may be incomplete or missing, and some jars omit their
META-INF/LICENSE file. Cross-check the summary.tsv output against the
authoritative LICENSE-binary in the repo root.

Modeled after Apache Flink's tools/releasing/collect_license_files.sh:
  https://github.com/apache/flink/blob/master/tools/releasing/collect_license_files.sh

Usage:
  python3 tools/licensing/collect_binary_licenses.py <jar-dir> [<out-dir>]

Example:
  # Extract lib/ from a running Docker image first:
  docker create --name tmp bobbai2000/texera-workflow-execution-coordinator:latest
  docker cp tmp:/texera/amber/lib /tmp/texera-lib
  docker rm tmp
  python3 tools/licensing/collect_binary_licenses.py /tmp/texera-lib /tmp/license-staging
"""

import re
import sys
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path


LICENSE_NAMES = ("LICENSE", "LICENSE.txt", "LICENSE.md", "license", "license.txt")
NOTICE_NAMES = ("NOTICE", "NOTICE.txt", "NOTICE.md", "notice", "notice.txt")


def find_member(zf: zipfile.ZipFile, candidate_basenames) -> str | None:
    """Find the first META-INF/<basename> that exists in the jar."""
    names = set(zf.namelist())
    for base in candidate_basenames:
        candidate = f"META-INF/{base}"
        if candidate in names:
            return candidate
    return None


def find_pom(zf: zipfile.ZipFile) -> str | None:
    """Find the jar's own pom.xml (under META-INF/maven/<group>/<artifact>/)."""
    for name in zf.namelist():
        if name.startswith("META-INF/maven/") and name.endswith("/pom.xml"):
            return name
    return None


def parse_pom_licenses(pom_bytes: bytes) -> list[tuple[str, str]]:
    """Return [(name, url)] pairs from a POM's <licenses> section."""
    try:
        text = pom_bytes.decode("utf-8", errors="replace")
        # Strip default namespace so XPath is readable
        text = re.sub(r' xmlns="[^"]+"', "", text, count=1)
        root = ET.fromstring(text)
        out = []
        for lic in root.findall("./licenses/license"):
            name = (lic.findtext("name") or "").strip()
            url = (lic.findtext("url") or "").strip()
            if name or url:
                out.append((name, url))
        return out
    except Exception:
        return []


def extract_to(jar: Path, member: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(jar) as zf:
        with zf.open(member) as src, open(dest, "wb") as out:
            out.write(src.read())


def process_jar(jar: Path, out_dir: Path):
    """Extract licensing artefacts from one jar. Returns a summary dict."""
    summary = {
        "jar": jar.name,
        "has_license": "",
        "has_notice": "",
        "pom_licenses": "",
    }
    try:
        with zipfile.ZipFile(jar) as zf:
            jar_out = out_dir / "bundled" / jar.stem
            jar_out.mkdir(parents=True, exist_ok=True)

            lic_member = find_member(zf, LICENSE_NAMES)
            if lic_member:
                extract_to(jar, lic_member, jar_out / "META-INF-LICENSE")
                summary["has_license"] = lic_member

            notice_member = find_member(zf, NOTICE_NAMES)
            if notice_member:
                extract_to(jar, notice_member, jar_out / "META-INF-NOTICE")
                summary["has_notice"] = notice_member

            pom_member = find_pom(zf)
            if pom_member:
                extract_to(jar, pom_member, jar_out / "pom.xml")
                with zf.open(pom_member) as pf:
                    licenses = parse_pom_licenses(pf.read())
                summary["pom_licenses"] = "; ".join(n for n, _ in licenses)

            # Remove the empty staging dir if the jar had no licensing info
            if not any(jar_out.iterdir()):
                jar_out.rmdir()
    except zipfile.BadZipFile:
        summary["has_license"] = "(not a zip)"
    return summary


def assemble_notice_skeleton(out_dir: Path) -> None:
    """Concatenate every extracted META-INF-NOTICE into NOTICE-binary-skeleton."""
    skeleton = out_dir / "NOTICE-binary-skeleton"
    bundled = out_dir / "bundled"
    with open(skeleton, "w", encoding="utf-8") as out:
        out.write(
            "# NOTICE-binary skeleton\n"
            "# Generated by tools/licensing/collect_binary_licenses.py\n"
            "# Manually curate before shipping: de-duplicate, group sensibly,\n"
            "# prepend the Apache Texera NOTICE header.\n\n"
        )
        if not bundled.exists():
            return
        for jar_dir in sorted(bundled.iterdir()):
            notice_file = jar_dir / "META-INF-NOTICE"
            if notice_file.exists():
                out.write(f"\n# ---- from {jar_dir.name} ----\n")
                out.write(notice_file.read_text(encoding="utf-8", errors="replace"))
                out.write("\n")


def write_summary(out_dir: Path, rows: list[dict]) -> None:
    summary_path = out_dir / "summary.tsv"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("jar\thas_license_file\thas_notice_file\tpom_licenses\n")
        for r in sorted(rows, key=lambda x: x["jar"]):
            f.write(
                f"{r['jar']}\t{r['has_license'] or '-'}\t"
                f"{r['has_notice'] or '-'}\t{r['pom_licenses'] or '-'}\n"
            )


def main(argv):
    if len(argv) < 2 or argv[1] in ("-h", "--help"):
        print(__doc__)
        return 0 if len(argv) > 1 else 1

    jar_dir = Path(argv[1])
    out_dir = Path(argv[2]) if len(argv) > 2 else Path("license-staging")

    if not jar_dir.is_dir():
        print(f"error: {jar_dir} is not a directory", file=sys.stderr)
        return 1

    jars = sorted(jar_dir.glob("*.jar"))
    if not jars:
        print(f"error: no jars found under {jar_dir}", file=sys.stderr)
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Scanning {len(jars)} jars in {jar_dir} -> {out_dir}")

    rows = [process_jar(j, out_dir) for j in jars]

    missing_lic = sum(1 for r in rows if not r["has_license"])
    missing_pom_lic = sum(1 for r in rows if not r["pom_licenses"])

    write_summary(out_dir, rows)
    assemble_notice_skeleton(out_dir)

    print(f"  jars scanned:                  {len(rows)}")
    print(f"  jars w/o META-INF/LICENSE:     {missing_lic}")
    print(f"  jars w/o POM license metadata: {missing_pom_lic}")
    print(f"  summary:                       {out_dir/'summary.tsv'}")
    print(f"  NOTICE skeleton:               {out_dir/'NOTICE-binary-skeleton'}")
    print(f"  per-jar extracts:              {out_dir/'bundled'}/<jar-name>/")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
