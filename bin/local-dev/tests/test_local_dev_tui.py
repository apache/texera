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

"""Unit tests for `bin/local-dev.sh -i` (the Textual dashboard).

We cover the pure helper functions that don't need a running Textual app:
version detection, docker state mapping, ANSI scrubbing, source-hash
fingerprinting, and the service catalog's structural invariants. The
interactive widgets are exercised manually."""

from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest


# ─────────────────── texera_version() ───────────────────

def test_texera_version_parses_build_sbt(tmp_path, monkeypatch, tui):
    sbt = tmp_path / "build.sbt"
    sbt.write_text(textwrap.dedent("""\
        ThisBuild / scalaVersion := "2.13.18"
        ThisBuild / version      := "9.9.9-FIXTURE"
        ThisBuild / organization := "org.apache.texera"
    """))
    monkeypatch.setattr(tui, "REPO_ROOT", tmp_path)
    monkeypatch.delenv("TEXERA_VERSION", raising=False)
    assert tui.texera_version() == "9.9.9-FIXTURE"


def test_texera_version_env_var_wins(tmp_path, monkeypatch, tui):
    (tmp_path / "build.sbt").write_text('ThisBuild / version := "ignored-by-env"\n')
    monkeypatch.setattr(tui, "REPO_ROOT", tmp_path)
    monkeypatch.setenv("TEXERA_VERSION", "from-env")
    assert tui.texera_version() == "from-env"


def test_texera_version_raises_when_build_sbt_missing(tmp_path, monkeypatch, tui):
    # No build.sbt in tmp_path. No fallback — must raise so a stale or
    # broken checkout doesn't silently pick the wrong version.
    monkeypatch.setattr(tui, "REPO_ROOT", tmp_path)
    monkeypatch.delenv("TEXERA_VERSION", raising=False)
    with pytest.raises(RuntimeError, match="build.sbt not found"):
        tui.texera_version()


def test_texera_version_raises_when_version_unparseable(tmp_path, monkeypatch, tui):
    # build.sbt exists but the `ThisBuild / version := "…"` line isn't there.
    (tmp_path / "build.sbt").write_text("name := \"texera\"\n")
    monkeypatch.setattr(tui, "REPO_ROOT", tmp_path)
    monkeypatch.delenv("TEXERA_VERSION", raising=False)
    with pytest.raises(RuntimeError, match="could not find.*version"):
        tui.texera_version()


# ─────────────────── docker_state() ───────────────────

@pytest.mark.parametrize("state,status,expected", [
    ("running",     "Up 5 minutes (healthy)",          "running"),
    ("running",     "Up 1s (health: starting)",        "starting"),
    ("running",     "Up 30s (unhealthy)",              "unhealthy"),
    ("running",     "Up 2 minutes",                    "running"),
    ("exited",      "Exited (0) 4 minutes ago",        "exited"),
    ("exited",      "Exited (137) 1 minute ago",       "failed"),
    ("created",     "Created",                         "starting"),
    ("restarting",  "Restarting (1) 2 seconds ago",    "starting"),
    ("paused",      "Up 10 minutes (Paused)",          "starting"),
    ("",            "",                                "stopped"),
    ("dead",        "Dead",                            "stopped"),
])
def test_docker_state_mapping(state, status, expected, tui):
    assert tui.docker_state(state, status) == expected


# ─────────────────── _strip_ansi_motion() ───────────────────

def test_strip_ansi_keeps_sgr_drops_motion(tui):
    raw = "\x1b[32m✓\x1b[0m hello \x1b[2K\x1b[Hworld\r\r"
    out = tui._strip_ansi_motion(raw)
    # SGR (\e[32m, \e[0m) must survive so colours render
    assert "\x1b[32m" in out
    assert "\x1b[0m" in out
    # cursor-positioning / erase / CR must be gone
    assert "\x1b[2K" not in out
    assert "\x1b[H" not in out
    assert "\r" not in out


def test_strip_ansi_idempotent(tui):
    plain = "no escape codes here"
    assert tui._strip_ansi_motion(plain) == plain


# ─────────────────── source_hash() / is_dirty() ───────────────────

def _seed_jvm_layout(repo: Path, svc_own_src: str, version: str = "9.9.9-T") -> None:
    """Drop the directory layout source_hash() / is_dirty() walk.

    Six shared `common/*/src` dirs + the per-service src dir + the dist
    artifact jar at `target/<svc>-<version>/lib/...` so the artifact-mtime
    lookup works."""
    for d in ["dao", "config", "auth", "workflow-core", "workflow-operator", "pybuilder"]:
        p = repo / "common" / d / "src"
        p.mkdir(parents=True, exist_ok=True)
        (p / "Stub.scala").write_text(f"// stub for common/{d}\n")
    own = repo / svc_own_src
    own.mkdir(parents=True, exist_ok=True)
    (own / "Main.scala").write_text("object Main\n")


def test_source_hash_stable_across_calls(tmp_path, monkeypatch, tui):
    monkeypatch.setattr(tui, "REPO_ROOT", tmp_path)
    _seed_jvm_layout(tmp_path, "config-service/src")
    svc = tui.SERVICES_BY_NAME["config-service"]
    a = tui.source_hash(svc)
    b = tui.source_hash(svc)
    assert a == b
    assert len(a) == 40  # SHA-1 hex


def test_source_hash_changes_with_content(tmp_path, monkeypatch, tui):
    monkeypatch.setattr(tui, "REPO_ROOT", tmp_path)
    _seed_jvm_layout(tmp_path, "config-service/src")
    svc = tui.SERVICES_BY_NAME["config-service"]
    before = tui.source_hash(svc)
    (tmp_path / "config-service/src/Main.scala").write_text("object Main { def x = 1 }\n")
    after = tui.source_hash(svc)
    assert before != after


def test_is_dirty_docker_always_clean(tui):
    svc = tui.SERVICES_BY_NAME["postgres"]
    assert tui.is_dirty(svc) is False


def test_is_dirty_after_seed_then_edit(tmp_path, monkeypatch, tui):
    monkeypatch.setattr(tui, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(tui, "BUILD_STAMP_DIR", tmp_path / "stamps")
    (tmp_path / "stamps").mkdir()
    _seed_jvm_layout(tmp_path, "config-service/src")

    # Drop in a "jar" so the lazy seed path kicks in (it just needs to exist;
    # the file's content isn't read by the dirty check, only the stamp's).
    svc = tui.SERVICES_BY_NAME["config-service"]
    jar = tmp_path / svc.artifact_jar
    jar.parent.mkdir(parents=True, exist_ok=True)
    jar.write_bytes(b"fake-jar-bytes")

    # First call seeds the stamp and reports clean.
    assert tui.is_dirty(svc) is False
    stamp = tmp_path / "stamps" / svc.name
    assert stamp.exists()
    seeded_hash = stamp.read_text().strip()
    assert len(seeded_hash) == 40

    # Edit source → next dirty check sees a hash mismatch.
    (tmp_path / "config-service/src/Main.scala").write_text("object Main { def y = 2 }\n")
    assert tui.is_dirty(svc) is True


def test_is_dirty_mtime_bump_without_content_change_stays_clean(tmp_path, monkeypatch, tui):
    """Robustness against `git checkout` touching mtimes — the whole reason we
    moved off pure-mtime detection. After seeding the stamp, simulating a
    checkout (re-touching source files without changing content) must NOT
    flash dirty."""
    monkeypatch.setattr(tui, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(tui, "BUILD_STAMP_DIR", tmp_path / "stamps")
    (tmp_path / "stamps").mkdir()
    _seed_jvm_layout(tmp_path, "config-service/src")
    svc = tui.SERVICES_BY_NAME["config-service"]
    jar = tmp_path / svc.artifact_jar
    jar.parent.mkdir(parents=True, exist_ok=True)
    jar.touch()

    tui.is_dirty(svc)  # seed

    # Simulate `git checkout` by touching all source files to "now" without
    # changing content.
    for f in (tmp_path / "config-service/src").rglob("*"):
        if f.is_file():
            os.utime(f, None)

    assert tui.is_dirty(svc) is False


# ─────────────────── Service catalog invariants ───────────────────

def test_service_names_unique(tui):
    names = [s.name for s in tui.SERVICES]
    assert len(names) == len(set(names)), "duplicate service name in catalog"


def test_service_ports_unique(tui):
    ports = [s.port for s in tui.SERVICES]
    assert len(ports) == len(set(ports)), "two services claim the same port"


def test_jvm_services_have_sbt_project_and_src(tui):
    for s in tui.SERVICES:
        if s.type == "jvm":
            # sbt_project is optional: sibling services (e.g.
            # computing-unit-master) ride another service's dist and have
            # no separate sbt invocation. own_src + artifact_jar are still
            # required so dirty-detection and start_one work.
            assert s.own_src, f"{s.name} missing own_src"
            assert s.artifact_jar, f"{s.name} missing artifact_jar"
            assert tui.TEXERA_VERSION in s.artifact_jar, (
                f"{s.name}'s jar path should embed the dynamic version, got {s.artifact_jar}"
            )


def test_jvm_siblings_share_artifact_jar(tui):
    """computing-unit-master is a launcher shipped inside amber's dist; it must
    point at the same canary jar as texera-web so dirty-state stays in sync."""
    cum = tui.SERVICES_BY_NAME["computing-unit-master"]
    web = tui.SERVICES_BY_NAME["texera-web"]
    assert cum.sbt_project is None, "computing-unit-master must not own an sbt project"
    assert cum.artifact_jar == web.artifact_jar
    assert cum.own_src == web.own_src


def test_docker_services_have_no_jar(tui):
    for s in tui.SERVICES:
        if s.type == "docker":
            assert s.artifact_jar is None


def test_watch_types_constant(tui):
    assert tui.WATCH_TYPES == {"yarn", "bun"}
    # Every watch-type service must show up in the catalog
    for t in tui.WATCH_TYPES:
        assert any(s.type == t for s in tui.SERVICES), f"no service of type {t}"


# ─────────────────── CommandHistory navigation ───────────────────

def test_command_history_back_and_forward(tmp_path, tui):
    h = tui.CommandHistory(history_file=tmp_path / "h")
    for cmd in ["u", "d", "config-service"]:
        h.push(cmd)
    assert h._history == ["u", "d", "config-service"]

    # ↑ from a fresh draft walks back from newest
    assert h.back("draft-typing") == "config-service"
    assert h.back("config-service") == "d"
    assert h.back("d") == "u"
    # At oldest, further ↑ returns None (caller keeps current value)
    assert h.back("u") is None

    # ↓ walks forward; once past newest it restores the draft
    assert h.forward() == "d"
    assert h.forward() == "config-service"
    assert h.forward() == "draft-typing"
    # At the live draft, ↓ is a no-op
    assert h.forward() is None


def test_command_history_dedup_consecutive(tmp_path, tui):
    h = tui.CommandHistory(history_file=tmp_path / "h")
    h.push("u")
    h.push("u")            # same as previous → not appended
    h.push("d")
    h.push("u")            # not consecutive, gets appended
    assert h._history == ["u", "d", "u"]


def test_command_history_persists(tmp_path, tui):
    f = tmp_path / "h"
    a = tui.CommandHistory(history_file=f)
    a.push("u")
    a.push("d")

    b = tui.CommandHistory(history_file=f)
    assert b._history == ["u", "d"]


def test_command_history_max_size(tmp_path, tui):
    h = tui.CommandHistory(history_file=tmp_path / "h", max_size=3)
    for cmd in ["a", "b", "c", "d", "e"]:
        h.push(cmd)
    reloaded = tui.CommandHistory(history_file=tmp_path / "h", max_size=3)
    # On-disk cap: only the most recent 3 survive a reload.
    assert reloaded._history == ["c", "d", "e"]


# ─────────────────── build.sbt dependency parsing ───────────────────

def test_sbt_graph_parsed_from_real_build_sbt(tui):
    """The parser must extract at least the well-known subset of projects
    (DAO, Config, Auth, the seven JVM services) and their direct deps from
    the live repo's build.sbt. If any of these go missing we've drifted
    from sbt's view of the world and dirty-detection is silently wrong."""
    g = tui._SBT_GRAPH
    assert "DAO" in g and "Config" in g and "Auth" in g
    assert "ConfigService" in g
    assert "WorkflowExecutionService" in g
    # Auth depends on DAO + Config — anchor invariant from build.sbt.
    assert set(g["Auth"]["deps"]) >= {"DAO", "Config"}
    # WorkflowCore depends on DAO/Config/PyBuilder.
    assert set(g["WorkflowCore"]["deps"]) >= {"DAO", "Config", "PyBuilder"}


def test_sbt_transitive_closure_skips_unrelated(tui):
    """config-service should NOT pull workflow-operator/workflow-core into
    its dep set. That's the whole point of using build.sbt as source of
    truth instead of a shared common/* list."""
    dirs = tui._transitive_src_dirs("ConfigService", tui._SBT_GRAPH)
    assert "common/workflow-operator/src" not in dirs
    assert "common/workflow-core/src" not in dirs
    # Should still include its own + Auth's reach (DAO/Config).
    assert "config-service/src" in dirs
    assert "common/auth/src" in dirs
    assert "common/dao/src" in dirs
    assert "common/config/src" in dirs


def test_sbt_transitive_for_amber_includes_workflow_chain(tui):
    """texera-web / computing-unit-master ride WorkflowExecutionService;
    its closure must include WorkflowOperator → WorkflowCore → DAO/Config/PyBuilder."""
    dirs = tui._transitive_src_dirs("WorkflowExecutionService", tui._SBT_GRAPH)
    expected = {
        "amber/src",
        "common/workflow-operator/src",
        "common/workflow-core/src",
        "common/dao/src",
        "common/config/src",
        "common/pybuilder/src",
        "common/auth/src",
    }
    assert expected <= set(dirs), f"missing: {expected - set(dirs)}"


def test_sbt_unknown_project_raises(tui):
    """Asking the graph for an unknown sbt project name raises rather
    than silently falling back to a hardcoded list — that fallback would
    mask a real build.sbt drift / typo."""
    with pytest.raises(RuntimeError, match="not found in build.sbt graph"):
        tui._transitive_src_dirs("NoSuchProject", tui._SBT_GRAPH)


def test_sbt_empty_graph_raises(tui):
    """A parse that returned no projects (very old / mangled build.sbt)
    must raise. Silently using the prior pre-parse list would let dirty
    detection report wrong results without anyone noticing."""
    with pytest.raises(RuntimeError, match="graph is empty"):
        tui._transitive_src_dirs("ConfigService", {})


def test_sbt_test_scope_deps_ignored(tui):
    """`X % "test->test"` is test-only — must not pollute the runtime dep
    list (services would otherwise rebuild on test-only edits, defeating
    the build skip)."""
    # Auth has a `.dependsOn(DAO % "test->test")` next to its main
    # `.dependsOn(DAO, Config)`. The test-scope reference should not
    # appear as a second entry.
    auth_deps = tui._SBT_GRAPH["Auth"]["deps"]
    # DAO appears once (from the main-scope dependsOn) — not twice from
    # the test-scope one.
    assert auth_deps.count("DAO") == 1
