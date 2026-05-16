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

"""Unit tests for check_binary_deps.

Run via:
    python3 -m unittest bin.licensing.test_check_binary_deps
or:
    python3 -m unittest discover -s bin/licensing -p "test_*.py"

These tests use only the Python standard library — no pytest, no project
deps — so they can run in any CI job that has Python set up.
"""
from __future__ import annotations

import csv
import io
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import check_binary_deps as cbd  # noqa: E402


# --- pure-function tests ---------------------------------------------------


class IndexersPreserveAllVersions(unittest.TestCase):
    """Regression test for the bug where dict-keyed-by-name indexers
    silently dropped a duplicate-name entry. The combined LICENSE-binary
    on `main` legitimately claims the same artifact at two versions in
    97 cases (e.g. logback 1.2.x + 1.4.x); the indexer must preserve all
    of them."""

    def test_index_npm_keeps_multiple_versions(self):
        idx = cbd._index_npm({"react@17.0.0", "react@18.2.0", "lodash@4.17.21"})
        self.assertEqual(idx["react"], {"17.0.0", "18.2.0"})
        self.assertEqual(idx["lodash"], {"4.17.21"})

    def test_index_npm_handles_scoped_names(self):
        # `@scope/name@version` — the version separator is the LAST `@`.
        idx = cbd._index_npm({"@angular/core@18.0.0", "@angular/core@17.0.0"})
        self.assertEqual(idx["@angular/core"], {"17.0.0", "18.0.0"})

    def test_index_python_keeps_multiple_versions(self):
        idx = cbd._index_python({"numpy==2.1.0", "numpy==2.0.0", "pandas==2.2.3"})
        self.assertEqual(idx["numpy"], {"2.0.0", "2.1.0"})
        self.assertEqual(idx["pandas"], {"2.2.3"})

    def test_index_jar_keeps_multiple_versions(self):
        # Two versions of the same artifact, plus an unrelated one.
        idx = cbd._index_jar({
            "ch.qos.logback.logback-classic-1.2.3.jar",
            "ch.qos.logback.logback-classic-1.4.14.jar",
            "io.netty.netty-buffer-4.1.96.Final.jar",
        })
        self.assertEqual(
            idx["ch.qos.logback.logback-classic"], {"1.2.3", "1.4.14"}
        )
        self.assertEqual(idx["io.netty.netty-buffer"], {"4.1.96.Final"})

    def test_index_jar_warns_on_unparseable_name(self):
        buf = io.StringIO()
        with redirect_stderr(buf):
            idx = cbd._index_jar({"weird-no-version.jar"})
        self.assertEqual(idx, {})
        self.assertIn("cannot parse jar name", buf.getvalue())


class JarBasenameRoundTrip(unittest.TestCase):
    """`_jar_basename` must reconstruct the exact basename that
    `JAR_NAME_VERSION` parsed, including for jars with classifier
    suffixes (the version regex captures the whole tail)."""

    def test_round_trip_simple(self):
        for jar in [
            "io.netty.netty-buffer-4.1.96.Final.jar",
            "commons-cli-1.5.0.jar",
            "scala-library-2.13.10.jar",
            "io.netty.netty-tcnative-boringssl-static-2.0.61.Final-linux-x86_64.jar",
            "co.fs2.fs2-core_2.13-3.12.2.jar",
        ]:
            with self.subTest(jar=jar):
                m = cbd.JAR_NAME_VERSION.match(jar)
                self.assertIsNotNone(m, f"failed to parse {jar}")
                self.assertEqual(cbd._jar_basename(m.group(1), m.group(2)), jar)


class IsDirectJar(unittest.TestCase):
    """`_is_direct_jar` reconciles SBT's bare artifactId with
    sbt-native-packager's `<groupId>.<artifactId>-<version>.jar` jar
    naming, plus Scala's `_<scalaVer>` suffix on `%%` libs."""

    direct = {"netty-buffer", "jersey-common", "fs2-core"}

    def test_group_prefixed(self):
        self.assertTrue(cbd._is_direct_jar("io.netty.netty-buffer", self.direct))
        self.assertTrue(cbd._is_direct_jar("org.glassfish.jersey.core.jersey-common", self.direct))

    def test_bare_artifact(self):
        self.assertTrue(cbd._is_direct_jar("netty-buffer", self.direct))

    def test_scala_suffix_on_group_prefixed(self):
        self.assertTrue(cbd._is_direct_jar("co.fs2.fs2-core_2.13", self.direct))

    def test_unknown_artifact(self):
        self.assertFalse(cbd._is_direct_jar("some.thing.unrelated", self.direct))
        self.assertFalse(cbd._is_direct_jar("unrelated", self.direct))


class DiffSimple(unittest.TestCase):
    """`diff_simple` (npm/python): added/stale must include version,
    drift must be reported per-name with both version sets."""

    def test_clean_no_diff(self):
        idx = {"a": {"1.0"}, "b": {"2.0"}}
        added, stale, dd, dt = cbd.diff_simple(idx, idx, set(), joiner="==")
        self.assertEqual((added, stale, dd, dt), ([], [], [], []))

    def test_added_and_stale_include_version(self):
        claim = {"a": {"1.0"}}
        real = {"b": {"2.0"}}
        added, stale, dd, dt = cbd.diff_simple(claim, real, set(), joiner="==")
        self.assertEqual(added, ["b==2.0"])
        self.assertEqual(stale, ["a==1.0"])
        self.assertEqual(dd, [])
        self.assertEqual(dt, [])

    def test_added_and_stale_emit_one_entry_per_version(self):
        # Brand-new package bundled at two versions: surface both.
        claim = {}
        real = {"newpkg": {"1.0", "2.0"}}
        added, stale, dd, dt = cbd.diff_simple(claim, real, set(), joiner="==")
        self.assertEqual(added, ["newpkg==1.0", "newpkg==2.0"])

    def test_single_version_drift_classified_direct_vs_transitive(self):
        claim = {"foo": {"1.0"}, "bar": {"1.0"}}
        real  = {"foo": {"1.1"}, "bar": {"1.1"}}
        added, stale, dd, dt = cbd.diff_simple(claim, real, {"foo"}, joiner="==")
        self.assertEqual(added, [])
        self.assertEqual(stale, [])
        self.assertEqual(dd, [("foo", ["1.0"], ["1.1"])])
        self.assertEqual(dt, [("bar", ["1.0"], ["1.1"])])

    def test_multi_version_drift_reports_both_sides(self):
        # The bug this PR fixes: previously these collapsed.
        claim = {"jetty": {"9.4.20", "11.0.20"}}
        real  = {"jetty": {"9.4.20", "11.0.21"}}
        _, _, _, dt = cbd.diff_simple(claim, real, set(), joiner="==")
        self.assertEqual(dt, [("jetty", ["11.0.20", "9.4.20"], ["11.0.21", "9.4.20"])])

    def test_npm_joiner(self):
        claim, real = {}, {"react": {"18.2.0"}}
        added, _, _, _ = cbd.diff_simple(claim, real, set(), joiner="@")
        self.assertEqual(added, ["react@18.2.0"])


class DiffJars(unittest.TestCase):
    """`diff_jars`: same shape as diff_simple but added/stale are full
    jar basenames (reconstructed via `_jar_basename`), and direct/
    transitive classification uses `_is_direct_jar`."""

    def test_clean(self):
        idx = {"io.netty.netty-buffer": {"4.1.96.Final"}}
        added, stale, dd, dt = cbd.diff_jars(idx, idx, set())
        self.assertEqual((added, stale, dd, dt), ([], [], [], []))

    def test_added_stale_use_full_basename(self):
        claim = {"a.b": {"1.0"}}
        real = {"x.y": {"2.0"}}
        added, stale, _, _ = cbd.diff_jars(claim, real, set())
        self.assertEqual(added, ["x.y-2.0.jar"])
        self.assertEqual(stale, ["a.b-1.0.jar"])

    def test_multi_version_added_stale_emits_one_basename_per_version(self):
        claim = {}
        real = {"io.netty.netty-buffer": {"4.1.96.Final", "4.1.100.Final"}}
        added, _, _, _ = cbd.diff_jars(claim, real, set())
        self.assertEqual(
            added,
            [
                "io.netty.netty-buffer-4.1.100.Final.jar",
                "io.netty.netty-buffer-4.1.96.Final.jar",
            ],
        )

    def test_drift_direct_vs_transitive_via_group_prefixed_match(self):
        # `netty-buffer` is direct (declared in SBT bare); the bundled jar
        # is `io.netty.netty-buffer` (sbt-native-packager naming).
        claim = {
            "io.netty.netty-buffer": {"4.1.96.Final"},
            "org.unknown.thing": {"1.0"},
        }
        real = {
            "io.netty.netty-buffer": {"4.1.100.Final"},
            "org.unknown.thing": {"1.1"},
        }
        _, _, dd, dt = cbd.diff_jars(claim, real, {"netty-buffer"})
        self.assertEqual(dd, [("io.netty.netty-buffer", ["4.1.96.Final"], ["4.1.100.Final"])])
        self.assertEqual(dt, [("org.unknown.thing", ["1.0"], ["1.1"])])


# --- end-to-end tests ------------------------------------------------------


def _write_lb(text: str) -> Path:
    p = Path(tempfile.mkstemp(suffix=".txt")[1])
    p.write_text(text)
    return p


def _write_pip_csv(rows: list[tuple[str, str]]) -> Path:
    p = Path(tempfile.mkstemp(suffix=".csv")[1])
    with p.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Name", "Version", "License"])
        for name, ver in rows:
            w.writerow([name, ver, "BSD"])
    return p


# Synthetic LICENSE-binary fixture mirroring the per-module file format
# (Apache-2 / MIT divider lines + `Python packages:` header + bullets).
# Two python packages claimed: one at one version, one at two versions.
SYNTHETIC_LB = textwrap.dedent("""\
    Apache header etc.

    --------------------------------------------------------------------------------
    Dependencies under the Apache License, Version 2.0
    --------------------------------------------------------------------------------

    Python packages:
      - direct-pkg==1.0.0
      - transitive-pkg==2.0.0
      - transitive-pkg==2.5.0
""")


class EndToEndPython(unittest.TestCase):
    """Run main() against a synthetic LICENSE-binary + pip-licenses CSV
    and assert the exit codes for each behavior class."""

    def setUp(self):
        self.lb = _write_lb(SYNTHETIC_LB)

    def _run(self, csv_rows: list[tuple[str, str]], *flags: str) -> int:
        # main() reads sys.argv; route stdout/stderr through buffers so
        # failures don't pollute test output.
        csv_path = _write_pip_csv(csv_rows)
        argv_save = sys.argv
        sys.argv = [
            "x", "--license-binary", str(self.lb), *flags,
            "python", str(csv_path),
        ]
        # Patch direct-deps loader to a known set rather than reading
        # the real repo's requirements.txt.
        loader_save = cbd.load_direct_python
        cbd.load_direct_python = lambda: {"direct-pkg"}
        try:
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                return cbd.main()
        finally:
            sys.argv = argv_save
            cbd.load_direct_python = loader_save

    def test_clean_passes(self):
        # Reality matches all 3 claimed (name, version) pairs.
        rc = self._run([
            ("direct-pkg", "1.0.0"),
            ("transitive-pkg", "2.0.0"),
            ("transitive-pkg", "2.5.0"),
        ])
        self.assertEqual(rc, 0)

    def test_transitive_drift_strict_fails(self):
        rc = self._run([
            ("direct-pkg", "1.0.0"),
            ("transitive-pkg", "2.0.0"),
            ("transitive-pkg", "2.6.0"),  # bumped from 2.5.0
        ])
        self.assertEqual(rc, 1)

    def test_transitive_drift_with_flag_passes(self):
        rc = self._run(
            [
                ("direct-pkg", "1.0.0"),
                ("transitive-pkg", "2.0.0"),
                ("transitive-pkg", "2.6.0"),
            ],
            "--ignore-transitive-version",
        )
        self.assertEqual(rc, 0)

    def test_direct_drift_with_flag_still_fails(self):
        rc = self._run(
            [
                ("direct-pkg", "1.1.0"),  # bumped
                ("transitive-pkg", "2.0.0"),
                ("transitive-pkg", "2.5.0"),
            ],
            "--ignore-transitive-version",
        )
        self.assertEqual(rc, 1)

    def test_added_with_flag_still_fails(self):
        rc = self._run(
            [
                ("direct-pkg", "1.0.0"),
                ("transitive-pkg", "2.0.0"),
                ("transitive-pkg", "2.5.0"),
                ("brand-new", "9.9.9"),  # not claimed
            ],
            "--ignore-transitive-version",
        )
        self.assertEqual(rc, 1)

    def test_stale_with_flag_still_fails(self):
        # Drop both versions of transitive-pkg from reality.
        rc = self._run(
            [("direct-pkg", "1.0.0")],
            "--ignore-transitive-version",
        )
        self.assertEqual(rc, 1)

    def test_dropping_one_of_multi_versions_is_drift_not_stale(self):
        # transitive-pkg is still in reality (at one version); the missing
        # version is drift — passes with the flag.
        rc = self._run(
            [
                ("direct-pkg", "1.0.0"),
                ("transitive-pkg", "2.5.0"),
            ],
            "--ignore-transitive-version",
        )
        self.assertEqual(rc, 0)


class CollectNpmReturnsLicenses(unittest.TestCase):
    """`collect_npm` parses 3rdpartylicenses.json into (items, licenses).
    The license string is what the failure report quotes back to the user
    so they can verify ASF compatibility without leaving the CI log."""

    def _write_json(self, payload: list) -> Path:
        import json as _json
        p = Path(tempfile.mkstemp(suffix=".json")[1])
        p.write_text(_json.dumps(payload))
        return p

    def test_returns_items_and_licenses(self):
        p = self._write_json([
            {"name": "react", "version": "18.2.0", "license": "MIT"},
            {"name": "@angular/core", "version": "18.0.0", "license": "MIT"},
        ])
        items, licenses = cbd.collect_npm(p)
        self.assertEqual(items, {"react@18.2.0", "@angular/core@18.0.0"})
        self.assertEqual(licenses["react@18.2.0"], "MIT")
        self.assertEqual(licenses["@angular/core@18.0.0"], "MIT")

    def test_missing_license_field_omitted(self):
        # No 'license' key — the entry is still bundled (we must surface
        # it) but no license string is recorded.
        p = self._write_json([
            {"name": "obscure", "version": "1.0.0"},
        ])
        items, licenses = cbd.collect_npm(p)
        self.assertEqual(items, {"obscure@1.0.0"})
        self.assertNotIn("obscure@1.0.0", licenses)


class CollectPythonReturnsLicenses(unittest.TestCase):
    """`collect_python` parses pip-licenses CSV into (items, licenses)."""

    def test_returns_items_and_licenses(self):
        p = Path(tempfile.mkstemp(suffix=".csv")[1])
        with p.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["Name", "Version", "License"])
            w.writerow(["NumPy", "2.1.0", "BSD"])
            w.writerow(["requests", "2.31.0", "Apache 2.0"])
        items, licenses = cbd.collect_python(p)
        self.assertEqual(items, {"numpy==2.1.0", "requests==2.31.0"})
        self.assertEqual(licenses["numpy==2.1.0"], "BSD")
        self.assertEqual(licenses["requests==2.31.0"], "Apache 2.0")


class ReportRendersLicenseAndTargetFile(unittest.TestCase):
    """The failure report must include (a) each added bullet's declared
    license string when available, and (b) the target per-module file in
    both the per-bullet hint and the ACTION REQUIRED block — that's the
    actionable info that was missing before."""

    def _capture(self, **kwargs) -> str:
        buf = io.StringIO()
        with redirect_stdout(buf):
            cbd.report(**kwargs)
        return buf.getvalue()

    def test_added_line_includes_license_and_target(self):
        out = self._capture(
            added=["new-pkg@1.2.3"],
            stale=[],
            drift_direct=[],
            drift_transitive=[],
            label="npm packages",
            kind="npm",
            ignore_transitive_version=False,
            licenses={"new-pkg@1.2.3": "MIT"},
            target_file="frontend/LICENSE-binary",
        )
        self.assertIn("+ new-pkg@1.2.3", out)
        self.assertIn("(license: MIT)", out)
        self.assertIn("→ add to frontend/LICENSE-binary", out)
        self.assertIn("frontend/LICENSE-binary", out.split("ACTION REQUIRED", 1)[1])

    def test_added_without_license_still_renders_target(self):
        # Jar case: no license info available; we still tell the user
        # which file to update.
        out = self._capture(
            added=["foo-1.0.0.jar"],
            stale=[],
            drift_direct=[],
            drift_transitive=[],
            label="JVM jars",
            kind="jar",
            ignore_transitive_version=False,
            licenses={},
            target_file="amber/LICENSE-binary-java",
        )
        self.assertIn("+ foo-1.0.0.jar", out)
        self.assertNotIn("(license:", out)
        self.assertIn("→ add to amber/LICENSE-binary-java", out)

    def test_stale_and_drift_include_target_file_hint(self):
        out = self._capture(
            added=[],
            stale=["gone-pkg==1.0"],
            drift_direct=[("foo", ["1.0"], ["1.1"])],
            drift_transitive=[("bar", ["2.0"], ["2.1"])],
            label="Python packages",
            kind="python",
            ignore_transitive_version=False,
            licenses={},
            target_file="amber/LICENSE-binary-python",
        )
        self.assertIn("- gone-pkg==1.0", out)
        self.assertIn("→ remove from amber/LICENSE-binary-python", out)
        self.assertIn("→ update in amber/LICENSE-binary-python", out)

    def test_no_target_file_falls_back_to_generic_text(self):
        out = self._capture(
            added=["new-pkg@1.0"],
            stale=[],
            drift_direct=[],
            drift_transitive=[],
            label="npm packages",
            kind="npm",
            ignore_transitive_version=False,
            licenses={"new-pkg@1.0": "MIT"},
            target_file=None,
        )
        self.assertIn("(license: MIT)", out)
        self.assertNotIn("→ add to", out)


if __name__ == "__main__":
    unittest.main()
