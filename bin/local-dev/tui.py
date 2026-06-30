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

"""bin/local-dev.sh -i — Textual dashboard for the Texera local dev stack.

Lives next to bin/local-dev.sh; that shell script remains the canonical
engine (build, start, stop, status) and this TUI shells out to it for every
action. The dashboard itself owns state polling, dirty-source detection,
and the prompt loop. Textual handles diff rendering so the screen doesn't
accrete in scrollback the way the old zsh `\\e[H` redraw did.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import re
import shlex
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from rich.text import Text
from textual import events, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.reactive import reactive
from textual.widgets import DataTable, Input, RichLog, Static

# ─────────────────── Constants ───────────────────

REPO_ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = Path(os.environ.get("TEXERA_LOCAL_DEV_DIR", "/tmp/texera-local-dev"))
LOG_DIR = STATE_DIR / "logs"
BUILD_STAMP_DIR = STATE_DIR / "build-stamps"
# Per-service phase markers written by the shell during stop/build/start.
# Each file holds `<phase>\t<epoch>` — we read it back in _tick_state and
# render an animated transitional STATE if it's recent (<90s).
PHASE_DIR = STATE_DIR / "svc-phase"
PHASE_STALE_S = 90.0
REPL_LOG = LOG_DIR / "repl.log"
LOG_DIR.mkdir(parents=True, exist_ok=True)
BUILD_STAMP_DIR.mkdir(parents=True, exist_ok=True)

LOCAL_DEV_SH = REPO_ROOT / "bin" / "local-dev.sh"
DOCKER_PROJECT = "texera-local-dev"

HISTORY_FILE = STATE_DIR / "tui-history"
MAX_HISTORY = 500

SOURCE_SUFFIXES = {".scala", ".java", ".proto"}

# Single source of truth for per-JVM-service source dirs is the SBT build
# graph in build.sbt. We parse it instead of hardcoding `common/*` so that
# adding a `lazy val NewCommon = (project in file("common/new"))` and a
# corresponding `.dependsOn(NewCommon)` automatically flows through to
# dirty-detection without anyone touching this file.

# Matches test-scope dependsOn args we want to drop from the runtime
# graph. `X % "test->test"`, `X % "test"`, `X % Test`. Tighter than the
# previous "any %" check, which would have silently dropped a future
# `.dependsOn(X % "compile->compile")` and broken dirty-detection on X.
# No trailing `\b` — `\b` after a literal `"` doesn't match because both
# sides are non-word chars; we anchor the end of the quoted form on the
# closing `"` itself, and the `Test` form on (?:\b|$).
_TEST_SCOPE_RE = re.compile(r'%\s*(?:"test(?:->[^"]*)?"|Test(?:\b|$))')


def _parse_sbt_deps() -> dict[str, dict]:
    """Walk `build.sbt` and return a dependency graph keyed by sbt project
    name. Each entry is `{"path": <repo-relative dir>, "deps": [..main-scope
    deps..]}`. Test-only `% "test->test"` deps are filtered out — they
    can't affect a service's runtime artifact, so they don't drive dirty.
    """
    bs = REPO_ROOT / "build.sbt"
    try:
        text = bs.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return {}

    proj_re = re.compile(
        r'^lazy\s+val\s+([A-Z][A-Za-z0-9]*)\s*=\s*\(project\s+in\s+file\("([^"]+)"\)\)',
        re.MULTILINE,
    )
    matches = list(proj_re.finditer(text))
    if not matches:
        return {}

    graph: dict[str, dict] = {}
    for i, m in enumerate(matches):
        name = m.group(1)
        path = m.group(2)
        # The "block" for this project spans from this match to the next
        # `lazy val ... = (project in file(...))` (or end of file). All the
        # chained `.dependsOn(...)` calls live in there.
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        block = text[m.start():end]

        deps: list[str] = []
        for arg_list in re.findall(r"\.dependsOn\(([^)]*)\)", block):
            for arg in arg_list.split(","):
                arg = arg.strip()
                # Skip ONLY test-scope deps — match `% "test->test"` and
                # `% Test` exactly. Filtering all `%`-scoped args was too
                # broad: a future `.dependsOn(X % "compile->compile")` or
                # `% Provided` would silently disappear from the dep
                # graph and break dirty-detection on `X`.
                if _TEST_SCOPE_RE.search(arg):
                    continue
                # Strip any trailing whitespace / comments. Project refs
                # are bare identifiers.
                ref = arg.split()[0] if arg else ""
                if ref and ref[0].isupper():
                    deps.append(ref)
        graph[name] = {"path": path, "deps": deps}
    return graph


def _transitive_src_dirs(sbt_project: Optional[str], graph: dict[str, dict]) -> list[str]:
    """Return source dirs for sbt_project AND every project it transitively
    depends on. Each dir is `<project_path>/src` (the canonical sbt source
    location). Order is stable; duplicates removed. Raises if the graph
    is empty or `sbt_project` isn't in it — silently falling back to a
    hardcoded common/* list would mask a real build.sbt drift."""
    if not graph:
        raise RuntimeError("build.sbt dependency graph is empty — parse failed")
    if not sbt_project or sbt_project not in graph:
        raise RuntimeError(
            f"sbt project {sbt_project!r} not found in build.sbt graph; "
            f"known projects: {sorted(graph.keys())}"
        )
    visited: set[str] = set()
    out: list[str] = []

    def walk(name: str) -> None:
        if name in visited or name not in graph:
            return
        visited.add(name)
        path = graph[name]["path"]
        src = f"{path}/src"
        if src not in out:
            out.append(src)
        for d in graph[name]["deps"]:
            walk(d)
    walk(sbt_project)
    return out


_SBT_GRAPH = _parse_sbt_deps()


# Per-sbt-project transitive closure, memoised. Computed once on first
# touch; subsequent dirty checks are O(1) lookup vs the prior O(deps)
# BFS per call.
_SBT_TRANSITIVE_CACHE: dict[str, list[str]] = {}


def _transitive_src_dirs_cached(sbt_project: Optional[str]) -> list[str]:
    if sbt_project is None:
        return _transitive_src_dirs(None, _SBT_GRAPH)
    if sbt_project not in _SBT_TRANSITIVE_CACHE:
        _SBT_TRANSITIVE_CACHE[sbt_project] = _transitive_src_dirs(sbt_project, _SBT_GRAPH)
    return _SBT_TRANSITIVE_CACHE[sbt_project]

POLL_INTERVAL_S = 1.0     # how often to refresh service state
DIRTY_INTERVAL_S = 2.0    # how often to recompute dirty indicators

# Services whose own runtime watches the filesystem and rebuilds on source
# change: `yarn start` runs `ng serve` (Angular dev server, hot-reload) and
# `bun run --watch` reloads the agent-service on change.  The dashboard
# surfaces this in the SRC column so the user doesn't try to bounce them
# unnecessarily; the ★ "dirty" indicator only flashes for them when the lock
# file changes (i.e. an actual dep refresh is needed).
WATCH_TYPES = {"yarn", "bun"}


# ─────────────────── Texera version (dynamic) ───────────────────

_VERSION_RE = re.compile(
    r'^\s*ThisBuild\s*/\s*version\s*:=\s*"([^"]+)"', re.MULTILINE
)


def texera_version() -> str:
    """Parse the project version from build.sbt so artifact paths track
    whatever branch the developer is on (it's 1.3.0-incubating-SNAPSHOT on
    main today, was 1.2.0-incubating on release/v1.2, will differ again).
    Override with `TEXERA_VERSION` env var to bypass parsing."""
    env = os.environ.get("TEXERA_VERSION")
    if env:
        return env
    bs = REPO_ROOT / "build.sbt"
    if not bs.exists():
        raise RuntimeError(
            f"build.sbt not found at {bs} — set TEXERA_VERSION to bypass"
        )
    m = _VERSION_RE.search(bs.read_text(errors="replace"))
    if not m:
        raise RuntimeError(
            f"could not find `ThisBuild / version := \"…\"` in {bs} — "
            f"set TEXERA_VERSION to bypass"
        )
    return m.group(1)


TEXERA_VERSION = texera_version()


# ─────────────────── Service catalog ───────────────────

@dataclass
class Service:
    name: str
    type: str             # "docker" | "jvm" | "yarn" | "bun"
    port: int
    sbt_project: Optional[str] = None     # for jvm
    own_src: Optional[str] = None         # for jvm
    artifact_jar: Optional[str] = None    # for jvm


def _jvm(name: str, port: int, project: Optional[str], own_src: str) -> Service:
    """sbt-native-packager lays the dist out as
    `target/<artifact>-<VERSION>/lib/org.apache.texera.<artifact>-<VERSION>.jar`
    for every subproject. amber is the exception: its sbt subproject is
    named `amber` (not `texera-web`) and the dist goes under `amber/target/`
    rather than the repo-level `target/`. computing-unit-master rides that
    same amber dist as a sibling launcher — its `project` is None because
    no separate sbt invocation produces it."""
    is_amber_svc = name in ("texera-web", "computing-unit-master")
    artifact = "amber" if is_amber_svc else name
    target_prefix = "amber/" if is_amber_svc else ""
    jar = (
        f"{target_prefix}target/{artifact}-{TEXERA_VERSION}/lib/"
        f"org.apache.texera.{artifact}-{TEXERA_VERSION}.jar"
    )
    return Service(name, "jvm", port, sbt_project=project,
                   own_src=own_src, artifact_jar=jar)


SERVICES: list[Service] = [
    Service("postgres",   "docker", 5432),
    Service("minio",      "docker", 9000),
    Service("lakefs",     "docker", 8000),
    Service("lakekeeper", "docker", 8181),
    Service("litellm",    "docker", 4000),
    _jvm("config-service",                  9094, "ConfigService",
         "config-service/src"),
    _jvm("access-control-service",          9096, "AccessControlService",
         "access-control-service/src"),
    _jvm("file-service",                    9092, "FileService",
         "file-service/src"),
    _jvm("workflow-compiling-service",      9090, "WorkflowCompilingService",
         "workflow-compiling-service/src"),
    _jvm("computing-unit-master",           8085, None,
         "amber/src"),
    _jvm("computing-unit-managing-service", 8082, "ComputingUnitManagingService",
         "computing-unit-managing-service/src"),
    _jvm("texera-web",                      8080, "WorkflowExecutionService",
         "amber/src"),
    Service("agent-service", "bun",  3001),
    Service("frontend",      "yarn", 4200),
]

SERVICES_BY_NAME = {s.name: s for s in SERVICES}


# ─────────────────── Live state model ───────────────────

def read_svc_phase(svc_name: str) -> Optional[str]:
    """Return the shell-written phase (`stopping` / `building` / `starting`)
    or None if absent / stale. Stale-after-90s guards against a crashed
    `bin/local-dev.sh` leaving an orphan file."""
    f = PHASE_DIR / svc_name
    try:
        text = f.read_text().strip()
    except OSError:
        return None
    if not text:
        return None
    parts = text.split("\t", 1)
    phase = parts[0]
    try:
        ts = float(parts[1]) if len(parts) > 1 else 0.0
    except ValueError:
        ts = 0.0
    if ts and (time.time() - ts) > PHASE_STALE_S:
        return None
    return phase


@dataclass
class LiveState:
    """Snapshot of the world this tick — what the dashboard renders."""
    docker: dict[str, tuple[str, str]] = field(default_factory=dict)   # name -> (state, status)
    pids: dict[str, Optional[str]] = field(default_factory=dict)       # name -> pid or None
    dirty: dict[str, bool] = field(default_factory=dict)
    mtimes: dict[str, Optional[str]] = field(default_factory=dict)
    # Per-service resource usage (computed every poll for native services
    # via cheap `ps`; refreshed less often for docker via `docker stats`
    # since that call costs ~2s).
    uptimes: dict[str, str] = field(default_factory=dict)              # name -> "12m" / "—"
    cpu_pct: dict[str, str] = field(default_factory=dict)              # name -> "8.2%" / "—"
    mem_use: dict[str, str] = field(default_factory=dict)              # name -> "85M" / "—"
    phases:  dict[str, str] = field(default_factory=dict)              # name -> "stopping"/"building"/"starting" or ""


# ─────────────────── Helpers ───────────────────

async def _run_capture(*argv: str) -> str:
    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    stdout, _ = await proc.communicate()
    return stdout.decode(errors="replace")


async def lsof_port_pid(port: int) -> Optional[str]:
    out = await _run_capture("lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t")
    out = out.strip()
    return out.split("\n", 1)[0] if out else None


async def docker_ps_all() -> dict[str, tuple[str, str]]:
    out = await _run_capture(
        "docker", "compose", "-p", DOCKER_PROJECT, "ps", "-a",
        "--format", "{{.Service}}|{{.State}}|{{.Status}}",
    )
    result: dict[str, tuple[str, str]] = {}
    for line in out.splitlines():
        parts = line.split("|", 2)
        if len(parts) == 3:
            result[parts[0]] = (parts[1], parts[2])
    return result


def docker_state(svc_state: str, svc_status: str) -> str:
    """Map docker's raw state/status to the small palette the dashboard renders."""
    if svc_state == "running":
        if "(healthy)" in svc_status:
            return "running"
        if "(health: starting)" in svc_status:
            return "starting"
        if "(unhealthy)" in svc_status:
            return "unhealthy"
        return "running"
    if svc_state == "exited":
        return "exited" if svc_status.startswith("Exited (0)") else "failed"
    if svc_state in ("created", "restarting", "paused", "removing"):
        return "starting"
    return "stopped"


# ─────────────────── Dirty-source detection (content hash) ───────────────────

# ─────────────────── Uptime / resource helpers ───────────────────

def _format_uptime(secs: int) -> str:
    """Compact duration: `12s`, `5m 23s`, `2h 14m`, `3d 4h`."""
    if secs < 0:
        return "—"
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m {secs % 60}s"
    if secs < 86400:
        return f"{secs // 3600}h {(secs % 3600) // 60}m"
    return f"{secs // 86400}d {(secs % 86400) // 3600}h"


def _format_bytes(n: int) -> str:
    """Bytes → short human string (`85M`, `1.2G`, `467K`)."""
    if n < 1024:
        return f"{n}B"
    if n < 1024 ** 2:
        return f"{n // 1024}K"
    if n < 1024 ** 3:
        return f"{n // (1024 ** 2)}M"
    return f"{n / (1024 ** 3):.1f}G"


def _parse_etime(et: str) -> int:
    """`ps -o etime` format `[[DD-]hh:]mm:ss` → seconds."""
    et = et.strip()
    if not et:
        return -1
    days = 0
    if "-" in et:
        d, et = et.split("-", 1)
        days = int(d or 0)
    parts = et.split(":")
    try:
        if len(parts) == 3:
            h, m, s = (int(p or 0) for p in parts)
        elif len(parts) == 2:
            h = 0
            m, s = (int(p or 0) for p in parts)
        else:
            h = m = 0
            s = int(parts[0] or 0)
    except ValueError:
        return -1
    return days * 86400 + h * 3600 + m * 60 + s


def _docker_container_for(svc_name: str) -> str:
    """Container names: most are `texera-<svc>`; `litellm` is unprefixed."""
    if svc_name == "litellm":
        return "litellm"
    return f"texera-{svc_name}"


_DOCKER_STATS_CACHE: dict[str, tuple[str, str]] = {}
_DOCKER_STATS_TS: float = 0.0
_DOCKER_STATS_TTL = 5.0  # seconds — docker stats --no-stream costs ~2s, don't run it more than this


def _refresh_docker_stats() -> None:
    """Single `docker stats --no-stream` pass; refreshes the cached
    container → (cpu%, mem) map. Called from a background worker so the
    main poll tick stays cheap."""
    global _DOCKER_STATS_CACHE, _DOCKER_STATS_TS
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream",
             "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}"],
            capture_output=True, text=True, timeout=8,
        ).stdout
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return
    fresh: dict[str, tuple[str, str]] = {}
    for line in out.splitlines():
        parts = line.split("|")
        if len(parts) != 3:
            continue
        name, cpu, memu = parts
        # MemUsage is "85.2MiB / 1.95GiB" — strip the "/ total" half.
        used = memu.split(" /")[0].strip()
        # Normalise to our own _format_bytes shape: parse the IEC unit suffix.
        m = re.match(r"^([0-9.]+)\s*([KMGTPE]?i?B)?$", used)
        if m:
            val = float(m.group(1))
            unit = (m.group(2) or "B").lower()
            mult = {
                "b":    1,
                "kb":   1024,    "kib": 1024,
                "mb":   1024**2, "mib": 1024**2,
                "gb":   1024**3, "gib": 1024**3,
                "tb":   1024**4, "tib": 1024**4,
            }.get(unit, 1)
            mem_str = _format_bytes(int(val * mult))
        else:
            mem_str = used or "—"
        fresh[name] = (cpu, mem_str)
    _DOCKER_STATS_CACHE = fresh
    _DOCKER_STATS_TS = time.time()


def proc_uptime_cpu_mem(pid: str) -> tuple[str, str, str]:
    """For a native PID return (uptime, cpu%, mem) — `—` if not running."""
    try:
        out = subprocess.run(
            ["ps", "-p", pid, "-o", "etime=,pcpu=,rss="],
            capture_output=True, text=True, timeout=2,
        ).stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return ("—", "—", "—")
    if not out:
        return ("—", "—", "—")
    parts = out.split()
    if len(parts) < 3:
        return ("—", "—", "—")
    etime, cpu, rss_kb = parts[0], parts[1], parts[2]
    secs = _parse_etime(etime)
    try:
        mem = _format_bytes(int(rss_kb) * 1024)
    except ValueError:
        mem = "—"
    return (_format_uptime(secs), f"{cpu}%", mem)


def container_uptime(svc_name: str) -> str:
    """ISO StartedAt → human duration. Empty if not started."""
    container = _docker_container_for(svc_name)
    try:
        out = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.StartedAt}}", container],
            capture_output=True, text=True, timeout=2,
        ).stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return "—"
    if not out or out.startswith("0001-01-01"):
        return "—"
    try:
        # ISO 8601 with fractional + Z. Trim to the second.
        trimmed = out.split(".", 1)[0].rstrip("Z")
        started = datetime.strptime(trimmed, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return "—"
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    return _format_uptime(max(0, int(elapsed)))


def _jvm_src_dirs(svc: Service) -> list[Path]:
    # Derive the source-dir set from the SBT dependency graph in build.sbt.
    # Services that share another's sbt project (e.g. computing-unit-master
    # rides amber's dist) enter the graph at the *producing* project via
    # SHARED_SBT_PROJECT.
    SHARED_SBT_PROJECT = {
        "computing-unit-master": "WorkflowExecutionService",
    }
    project = svc.sbt_project or SHARED_SBT_PROJECT.get(svc.name)
    if not project:
        raise RuntimeError(
            f"service {svc.name!r} has no sbt_project and no SHARED_SBT_PROJECT "
            f"mapping — can't compute its source-dir closure"
        )
    src_dirs = _transitive_src_dirs_cached(project)
    return [REPO_ROOT / d for d in src_dirs if (REPO_ROOT / d).exists()]


def _jvm_source_files(svc: Service) -> list[Path]:
    files: list[Path] = []
    for d in _jvm_src_dirs(svc):
        for f in d.rglob("*"):
            if f.is_file() and f.suffix in SOURCE_SUFFIXES:
                files.append(f)
    # Sort by raw bytes of the string representation so the order matches
    # GNU `sort -z` exactly (the shell-side `svc_source_hash` uses that).
    # Python's default `Path.sort()` is component-wise and could disagree
    # at e.g. `Foo.scala` vs `Foo/` sibling-name collisions, which would
    # silently break stamp agreement between the two implementations.
    files.sort(key=lambda p: str(p).encode())
    return files


def source_hash(svc: Service, files: Optional[list[Path]] = None) -> str:
    """SHA-1 of every source byte under `svc`'s transitive sbt deps.
    Pass a pre-computed `files` list to skip the rglob walk — the
    dirty-check fast path already needs the file list to compare mtimes
    against the stamp, no point rglob'ing twice."""
    if files is None:
        files = _jvm_source_files(svc)
    h = hashlib.sha1()
    for f in files:
        try:
            h.update(f.read_bytes())
        except OSError:
            pass
    return h.hexdigest()


def _newest_mtime_after(files: list[Path], stamp_mtime: float) -> bool:
    for f in files:
        try:
            if f.stat().st_mtime > stamp_mtime:
                return True
        except OSError:
            continue
    return False


def is_dirty(svc: Service) -> bool:
    """Did the service's relevant source change since the last build?

    JVM: SHA-1 of all .scala/.java/.proto bytes vs. the hash we wrote at the
    last build, with an mtime fast filter so 99% of ticks are O(stat).
    yarn/bun: lock vs. node_modules dir mtime — cheap.
    docker: always clean.
    """
    if svc.type == "docker":
        return False
    if svc.type == "jvm":
        return _jvm_is_dirty(svc)
    if svc.type == "yarn":
        nm = REPO_ROOT / "frontend" / "node_modules" / ".yarn-state.yml"
        lock = REPO_ROOT / "frontend" / "yarn.lock"
        if not nm.exists() or not lock.exists():
            return True
        return lock.stat().st_mtime > nm.stat().st_mtime
    if svc.type == "bun":
        nm = REPO_ROOT / "agent-service" / "node_modules"
        lock = REPO_ROOT / "agent-service" / "bun.lock"
        if not nm.exists() or not lock.exists():
            return True
        return lock.stat().st_mtime > nm.stat().st_mtime
    return False


def _jvm_is_dirty(svc: Service) -> bool:
    stamp = BUILD_STAMP_DIR / svc.name
    if not stamp.exists() or stamp.stat().st_size == 0:
        # Lazy seed: if a jar is present, assume it matches current source and
        # write the hash. First REPL after a fresh checkout pays this once.
        jar = REPO_ROOT / svc.artifact_jar if svc.artifact_jar else None
        if jar is None or not jar.exists():
            return True
        stamp.write_text(source_hash(svc))
        return False

    files = _jvm_source_files(svc)
    stamp_mtime = stamp.stat().st_mtime

    # Fast filter: any source newer than the stamp?  If not, definitely clean.
    if not _newest_mtime_after(files, stamp_mtime):
        return False

    # Slow path — did the content actually move? Reuse the file list above
    # instead of having `source_hash` rglob the tree a second time.
    stored = stamp.read_text().strip()
    current = source_hash(svc, files)
    if current == stored:
        # Same content, only mtimes moved (git checkout / touch).  Refresh
        # the stamp's mtime so the fast filter passes next tick.
        os.utime(stamp, None)
        return False
    return True


def artifact_mtime_str(svc: Service) -> Optional[str]:
    if svc.type == "jvm" and svc.artifact_jar:
        jar = REPO_ROOT / svc.artifact_jar
        if jar.exists():
            return datetime.fromtimestamp(jar.stat().st_mtime).strftime("%m-%d %H:%M")
        return None
    if svc.type == "bun":
        f = REPO_ROOT / "agent-service" / "bun.lock"
    elif svc.type == "yarn":
        f = REPO_ROOT / "frontend" / "yarn.lock"
    else:
        return None
    if f.exists():
        return datetime.fromtimestamp(f.stat().st_mtime).strftime("%m-%d %H:%M")
    return None


# ─────────────────── Banner state (cheap) ───────────────────

def git_head() -> tuple[str, str]:
    branch = subprocess_run("git", "-C", str(REPO_ROOT), "rev-parse", "--abbrev-ref", "HEAD") or "?"
    sha = subprocess_run("git", "-C", str(REPO_ROOT), "rev-parse", "--short", "HEAD") or "?"
    return branch, sha


def worktree_info() -> tuple[str, bool]:
    """Return (label, is_worktree).  Label is the leaf directory name of the
    checkout — for the canonical clone this is `texera`, for a worktree it
    matches the worktree's directory name (which by our convention reflects
    the branch).  is_worktree distinguishes the main checkout from extras so
    the banner can flag it."""
    name = REPO_ROOT.name
    git_dir = subprocess_run("git", "-C", str(REPO_ROOT), "rev-parse", "--git-dir")
    common_dir = subprocess_run("git", "-C", str(REPO_ROOT), "rev-parse", "--git-common-dir")
    is_worktree = False
    try:
        if git_dir and common_dir:
            g = Path(git_dir) if Path(git_dir).is_absolute() else (REPO_ROOT / git_dir)
            c = Path(common_dir) if Path(common_dir).is_absolute() else (REPO_ROOT / common_dir)
            is_worktree = g.resolve() != c.resolve()
    except Exception:
        pass
    return name, is_worktree


def subprocess_run(*argv: str) -> str:
    import subprocess as sp
    try:
        return sp.check_output(argv, stderr=sp.DEVNULL, text=True).strip()
    except Exception:
        return ""


# ─────────────────── Input with shell-style history ───────────────────

class CommandHistory:
    """Pure-Python state machine for command history navigation.

    Kept separate from `HistoricInput` (which subclasses Textual's `Input`)
    so the navigation logic can be unit-tested without a running app —
    Textual's reactive setters need an active App context, so they can't
    be exercised from a bare pytest. `HistoricInput` is a thin wrapper that
    delegates here and forwards the resulting value to its `Input.value`.

    Conventions match bash/zsh: ↑ walks back from newest to oldest, the
    in-progress draft is saved when stepping off it the first time, ↓
    walks forward and restores the draft once you step past the newest
    entry. Consecutive duplicates are coalesced on `push`."""

    def __init__(self, history_file: Optional[Path] = None, max_size: int = MAX_HISTORY) -> None:
        self._file = history_file
        self._max = max_size
        self._history: list[str] = self._load()
        self._idx: int = -1   # -1 = at the live draft; 0+ = back in history
        self._draft: str = ""

    def _load(self) -> list[str]:
        if not self._file or not self._file.exists():
            return []
        try:
            lines = self._file.read_text(errors="replace").splitlines()
            return [s for s in (l.strip() for l in lines) if s][-self._max:]
        except OSError:
            return []

    def _save(self) -> None:
        if not self._file:
            return
        try:
            self._file.write_text("\n".join(self._history[-self._max:]) + "\n")
        except OSError:
            pass

    def push(self, cmd: str) -> None:
        cmd = cmd.strip()
        if not cmd:
            return
        if self._history and self._history[-1] == cmd:
            self._reset()
            return
        self._history.append(cmd)
        self._save()
        self._reset()

    def _reset(self) -> None:
        self._idx = -1
        self._draft = ""

    def back(self, current_value: str) -> Optional[str]:
        """Step one entry back in history. Returns the new value to display,
        or None if we're already at the oldest entry (caller should leave
        the input alone)."""
        if not self._history:
            return None
        if self._idx == -1:
            self._draft = current_value
        if self._idx + 1 >= len(self._history):
            return None
        self._idx += 1
        return self._history[-1 - self._idx]

    def forward(self) -> Optional[str]:
        """Step one entry forward. Returns the draft when you cross the
        newest entry. Returns None if we weren't browsing history."""
        if self._idx == -1:
            return None
        self._idx -= 1
        if self._idx == -1:
            return self._draft
        return self._history[-1 - self._idx]


class HistoricInput(Input):
    """Textual Input wired up to `CommandHistory` for shell-style ↑/↓.

    History is persisted to `HISTORY_FILE` under STATE_DIR so it survives
    across REPL sessions."""

    BINDINGS = [
        Binding("up",   "history_back",    "history back",    show=False),
        Binding("down", "history_forward", "history forward", show=False),
    ]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._hist = CommandHistory(HISTORY_FILE)

    def push(self, cmd: str) -> None:
        self._hist.push(cmd)

    def _set_value(self, v: str) -> None:
        self.value = v
        with contextlib.suppress(Exception):
            self.cursor_position = len(v)

    def action_history_back(self) -> None:
        v = self._hist.back(self.value)
        if v is not None:
            self._set_value(v)

    def action_history_forward(self) -> None:
        v = self._hist.forward()
        if v is not None:
            self._set_value(v)


# ─────────────────── Textual app ───────────────────

class LogResizeHandle(Static):
    """The one-row grip above the log pane. Drag it up/down with the mouse
    to resize the log; the height is clamped by the RichLog CSS min/max.
    Doubles as the static "drag to resize" label when idle."""

    DEFAULT_LABEL = " ⇅  log  (drag this row to resize)"

    def __init__(self, **kwargs) -> None:
        super().__init__(self.DEFAULT_LABEL, **kwargs)
        self._dragging = False
        self._drag_origin_y = 0
        self._drag_origin_h = 0

    def on_mouse_down(self, event: events.MouseDown) -> None:
        # We only care about the primary button.
        if event.button != 1:
            return
        log = self.app.query_one("#log", RichLog)
        # `region.height` is the current rendered height in cells. Lock it
        # in as the baseline so we can compute deltas without races against
        # auto-layout while the user drags.
        self._drag_origin_y = event.screen_y
        self._drag_origin_h = log.region.height
        self._dragging = True
        self.capture_mouse(True)
        # Replace the label so users know they grabbed it.
        self.update(f" ⇅  resizing… (height {self._drag_origin_h})")

    def on_mouse_move(self, event: events.MouseMove) -> None:
        if not self._dragging:
            return
        # Dragging UP (smaller screen_y) should GROW the log since the log
        # sits below this handle. Invert the delta and clamp; CSS max/min
        # are a fallback but the explicit clamp keeps the label honest.
        delta = self._drag_origin_y - event.screen_y
        new_h = max(4, min(60, int(self._drag_origin_h + delta)))
        self.app.query_one("#log", RichLog).styles.height = new_h
        self.update(f" ⇅  resizing… (height {new_h})")

    def on_mouse_up(self, event: events.MouseUp) -> None:
        if not self._dragging:
            return
        self._dragging = False
        self.capture_mouse(False)
        self.update(self.DEFAULT_LABEL)


# "Apache Texera" wordmark in box-drawing block characters. Single line,
# 6 rows × ~104 cols. Coloured teal to match the brand.
LOGO_TEXERA = (
    " █████╗ ██████╗  █████╗  ██████╗██╗  ██╗███████╗    ████████╗███████╗██╗  ██╗███████╗██████╗  █████╗ \n"
    "██╔══██╗██╔══██╗██╔══██╗██╔════╝██║  ██║██╔════╝    ╚══██╔══╝██╔════╝╚██╗██╔╝██╔════╝██╔══██╗██╔══██╗\n"
    "███████║██████╔╝███████║██║     ███████║█████╗         ██║   █████╗   ╚███╔╝ █████╗  ██████╔╝███████║\n"
    "██╔══██║██╔═══╝ ██╔══██║██║     ██╔══██║██╔══╝         ██║   ██╔══╝   ██╔██╗ ██╔══╝  ██╔══██╗██╔══██║\n"
    "██║  ██║██║     ██║  ██║╚██████╗██║  ██║███████╗       ██║   ███████╗██╔╝ ██╗███████╗██║  ██║██║  ██║\n"
    "╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚══════╝       ╚═╝   ╚══════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝"
)


STATE_STYLE = {
    "running":   ("●", "green"),
    "starting":  ("⚠", "yellow"),
    "unhealthy": ("✗", "red"),
    "failed":    ("✗", "red"),
    "exited":    ("✓", "grey50"),
    "stopped":   ("○", "grey50"),
}


def state_cell(state: str) -> Text:
    sym, style = STATE_STYLE.get(state, ("○", "grey50"))
    return Text(f"{sym}  {state}", style=style)


class LocalDevApp(App):
    """Live dashboard + REPL for the texera local dev stack."""

    CSS = """
    Screen { layout: vertical; }
    #banner {
        height: 11;
        background: $boost;
        color: $text;
        padding: 0 2;
        border-bottom: heavy $primary;
    }
    /* Collapsed mode: hide the wordmark, leave the title/sub two-liner.
       Use `Ctrl-B` to toggle — useful on short terminals where the dashboard
       gets cramped by the logo. */
    #banner.-collapsed {
        height: 3;
    }
    #banner.-collapsed #banner-logo {
        display: none;
    }
    #banner-logo  {
        color: #20b2aa;
        text-style: bold;
        height: 7;
    }
    #banner-title { text-style: bold; }
    #banner-sub  { color: $text-muted; }
    DataTable {
        height: 1fr;
        border-bottom: solid $primary-darken-1;
    }
    #log-header {
        height: 1;
        color: $text-muted;
        background: $primary-darken-3;
        padding: 0 2;
    }
    #log-header.-hidden { display: none; }
    #log-header:hover { color: $accent; background: $primary-darken-2; }
    RichLog {
        height: 18;
        min-height: 4;
        max-height: 60;
        background: $surface;
        scrollbar-size: 1 1;
        padding: 0 1;
    }
    RichLog.-hidden { display: none; }
    #status-bar {
        height: 1;
        background: $primary-darken-2;
        color: $text;
        padding: 0 2;
    }
    Input { background: $surface; }
    """

    BINDINGS = [
        # Ctrl-C: first press cancels the current command (or log tail); if
        # nothing's active, requires a second press within 2 s to actually
        # quit.  Matches the way shells & many TUIs treat it.
        Binding("ctrl+c", "soft_quit",      "Cancel / Quit",   priority=True, show=False),
        Binding("escape", "escape_view",    "Exit log view",   priority=True, show=False),
        Binding("ctrl+l", "clear_log",      "Clear log",                         show=False),
        Binding("ctrl+r", "manual_refresh", "Refresh",                            show=False),
        Binding("ctrl+b", "toggle_banner",  "Toggle banner",                      show=False),
    ]

    # Reactive state — Textual diffs widget content when these change.
    live: reactive[LiveState] = reactive(LiveState, recompose=False)
    active_cmd: reactive[Optional[str]] = reactive(None)
    cmd_started_at: reactive[float] = reactive(0.0)
    log_log_position: reactive[int] = reactive(0)   # byte offset we've already read

    # Non-reactive bookkeeping
    _branch: str = "?"
    _sha: str = "?"
    _last_dirty_check: float = 0.0
    _cached_dirty: dict[str, bool]
    _cached_mtimes: dict[str, Optional[str]]
    _cmd_proc: Optional[asyncio.subprocess.Process] = None

    def __init__(self) -> None:
        super().__init__()
        self._cached_dirty = {s.name: False for s in SERVICES}
        self._cached_mtimes = {s.name: None for s in SERVICES}
        self._branch, self._sha = git_head()
        self._worktree_name, self._is_worktree = worktree_info()
        self._log_visible = False
        self._log_auto_hide_handle = None  # type: ignore
        self._last_ctrl_c_ts: float = 0.0

    # ── Log visibility ──
    def _set_log_visible(self, show: bool) -> None:
        self._log_visible = show
        log = self.query_one("#log", RichLog)
        header = self.query_one("#log-header", LogResizeHandle)
        if show:
            log.remove_class("-hidden")
            header.remove_class("-hidden")
        else:
            log.add_class("-hidden")
            header.add_class("-hidden")

    # Column keys we keep so update_cell() can address cells reliably.  The
    # string labels passed to add_columns() are NOT the keys (Textual hands
    # back auto-generated ColumnKey objects), so doing
    # `update_cell(row, "STATE", ...)` silently fails.
    _COL_LABELS = ("●", "SERVICE", "PORT", "PID", "UPTIME", "CPU%", "MEM", "ARTIFACT", "BUILD", "STATE")
    _COL_KEYS   = ("sym", "svc",     "port", "pid", "uptime", "cpu",  "mem", "mtime",    "src",   "state")
    # Min width per column key. Without these, Textual sizes the column to
    # its header (e.g. PID = 3 chars), then truncates wider cell values
    # like "76348" → "763", which made every JVM look like it shared a
    # PID. Numeric/timestamp columns deserve enough room for the
    # widest plausible value.
    _COL_MIN_WIDTH = {
        "svc":    32,
        "port":   6,
        "pid":    7,    # up to 7-digit PIDs (Linux default max is 4194304)
        "uptime": 10,
        "cpu":    7,
        "mem":    7,
        "mtime":  18,
        "src":    5,
        "state":  12,
    }

    # ── Layout ──
    def compose(self) -> ComposeResult:
        yield Vertical(
            Static(LOGO_TEXERA, id="banner-logo"),
            Static("", id="banner-title"),
            Static("", id="banner-sub"),
            id="banner",
        )
        table = DataTable(zebra_stripes=False, cursor_type="row")
        for label, key in zip(self._COL_LABELS, self._COL_KEYS):
            table.add_column(label, key=key, width=self._COL_MIN_WIDTH.get(key))
        for s in SERVICES:
            table.add_row("○", s.name, f":{s.port}", "—", "—", "—", "—", "—", "  ", "stopped", key=s.name)
        yield table
        yield LogResizeHandle(id="log-header", classes="-hidden")
        yield RichLog(id="log", highlight=False, markup=False, wrap=False,
                      auto_scroll=True, classes="-hidden")
        yield Static("", id="status-bar")
        yield HistoricInput(placeholder="type a command (h for help · ↑/↓ history · q to quit)", id="prompt")

    def on_mount(self) -> None:
        self.title = "Texera Local Dev"
        self._update_banner()
        self._update_status_bar()
        self.query_one("#prompt", Input).focus()
        # Background polling tasks
        self.set_interval(POLL_INTERVAL_S, self._tick_state)
        self.set_interval(0.2, self._tick_log)
        self.set_interval(0.5, self._tick_banner)
        # 3 Hz lightweight tick so the `stopping.`/`building..` etc dots
        # animate smoothly between full state polls. Skips work entirely
        # when no phase markers are active, so the steady-state cost is a
        # 14-row dict lookup.
        self.set_interval(0.33, self._tick_phase_animation)
        # Kick the first poll immediately so the table populates fast.
        self.call_later(self._tick_state)
        self.call_later(self._tick_log)

    # ── Updates ──
    def _update_banner(self) -> None:
        now = datetime.now().strftime("%H:%M:%S")
        wt_tag = f"worktree: {self._worktree_name}" if self._is_worktree else f"checkout: {self._worktree_name}"
        sub = f"{wt_tag}  ·  branch: {self._branch} @ {self._sha}  ·  {now}"
        self.query_one("#banner-title", Static).update("Apache Texera — Local Dev")
        self.query_one("#banner-sub", Static).update(sub)

    def _update_status_bar(self) -> None:
        running = sum(
            1 for s in SERVICES
            if (s.type == "docker" and docker_state(*self.live.docker.get(s.name, ("", ""))) == "running")
            or (s.type != "docker" and self.live.pids.get(s.name))
        )
        total = len(SERVICES)
        dirty = sum(1 for d in self.live.dirty.values() if d)
        active = self.active_cmd or "idle"
        elapsed = ""
        if self.active_cmd and self.cmd_started_at:
            elapsed = f"  ({int(time.monotonic() - self.cmd_started_at)}s)"
        dirty_part = f"  ★ {dirty} dirty" if dirty else ""
        # Surface the frontend URL the moment ng serve is listening, so the
        # user knows when the web app is actually clickable.
        frontend_pid = self.live.pids.get("frontend")
        url_part = "  →  http://localhost:4200" if frontend_pid else ""
        self.query_one("#status-bar", Static).update(
            f"{running}/{total} running{dirty_part}{url_part}    last: {active}{elapsed}"
        )

    # State-cell renderer: when the shell has written a transitional phase
    # (stopping / building / starting), we display that with cycling dots
    # ("building." → "building.." → "building...") so the user sees motion
    # while the per-service action is in flight. Cycles at 3 Hz off the
    # main poll tick.
    _PHASE_PALETTE = {
        "stopping": ("◐", "yellow"),
        "building": ("⚙", "cyan"),
        "starting": ("⚠", "yellow"),
    }

    def _phase_text(self, phase: str) -> Text:
        sym, style = self._PHASE_PALETTE.get(phase, ("⚠", "yellow"))
        dots = "." * (int(time.monotonic() * 3) % 3 + 1)
        return Text(f"{phase}{dots}", style=style)

    def _refresh_table(self) -> None:
        table = self.query_one(DataTable)
        for svc in SERVICES:
            phase = self.live.phases.get(svc.name, "")
            if svc.type == "docker":
                ds, dstatus = self.live.docker.get(svc.name, ("", ""))
                state = docker_state(ds, dstatus)
                pid = "—"
            else:
                pid = self.live.pids.get(svc.name) or "—"
                state = "running" if self.live.pids.get(svc.name) else "stopped"
            # If the shell signalled a transitional phase AND we haven't
            # observed the service as fully running yet, render the phase
            # instead. Once the poller confirms "running" we trust that
            # over a stale stop/build/start marker.
            if phase and state != "running":
                sym, style = self._PHASE_PALETTE.get(phase, ("⚠", "yellow"))
                state_cell_text = self._phase_text(phase)
            else:
                sym, style = STATE_STYLE.get(state, ("○", "grey50"))
                state_cell_text = Text(state, style=style)
            mtime = self.live.mtimes.get(svc.name) or "—"
            dirty = self.live.dirty.get(svc.name, False)

            # BUILD column tells the user whether they need to do anything to
            # bring the running service in sync with the current source.
            #   ★ (yellow) — content/lock changed since last build; needs action.
            #   ↻ (cyan)   — service auto-rebuilds on file change (ng serve /
            #               bun --watch). Reassurance that no manual step is
            #               needed for source edits.
            #   (blank)    — built and up-to-date.
            if dirty:
                src_cell: Text | str = Text("★", style="bold yellow")
            elif svc.type in WATCH_TYPES:
                src_cell = Text("↻", style="cyan")
            else:
                src_cell = "  "

            uptime = self.live.uptimes.get(svc.name, "—")
            cpu    = self.live.cpu_pct.get(svc.name, "—")
            mem    = self.live.mem_use.get(svc.name, "—")
            table.update_cell(svc.name, "sym",    Text(sym, style=style))
            table.update_cell(svc.name, "port",   f":{svc.port}")
            table.update_cell(svc.name, "pid",    str(pid))
            table.update_cell(svc.name, "uptime", uptime)
            table.update_cell(svc.name, "cpu",    cpu)
            table.update_cell(svc.name, "mem",    mem)
            table.update_cell(svc.name, "mtime",  mtime)
            table.update_cell(svc.name, "src",    src_cell)
            table.update_cell(svc.name, "state",  state_cell_text)

    # ── Polling tasks (Textual will run them on the event loop) ──
    @work(exclusive=True, group="state")
    async def _tick_state(self) -> None:
        # Polling cost ≈ 1 docker compose ps + lsof × N native services, run
        # concurrently.  ~200 ms total on this box.
        docker_task = asyncio.create_task(docker_ps_all())
        native_tasks = {
            s.name: asyncio.create_task(lsof_port_pid(s.port))
            for s in SERVICES if s.type != "docker"
        }
        docker_map = await docker_task
        pids = {name: await task for name, task in native_tasks.items()}

        # Dirty check is more expensive; only re-do every DIRTY_INTERVAL_S.
        now = time.monotonic()
        if now - self._last_dirty_check >= DIRTY_INTERVAL_S:
            loop = asyncio.get_running_loop()
            self._cached_dirty = {
                s.name: await loop.run_in_executor(None, is_dirty, s) for s in SERVICES
            }
            self._cached_mtimes = {s.name: artifact_mtime_str(s) for s in SERVICES}
            self._last_dirty_check = now

        # Per-service uptime / cpu / mem.
        # Native: cheap `ps -o etime,pcpu,rss` per pid (<5ms each).
        # Docker: cached via `docker stats --no-stream` refreshed by a
        #         background worker every _DOCKER_STATS_TTL seconds (the
        #         call itself takes ~2s, too slow to run in-line each tick).
        uptimes: dict[str, str] = {}
        cpus:    dict[str, str] = {}
        mems:    dict[str, str] = {}
        for s in SERVICES:
            if s.type == "docker":
                state = docker_state(*docker_map.get(s.name, ("", "")))
                if state == "running":
                    uptimes[s.name] = container_uptime(s.name)
                    container = _docker_container_for(s.name)
                    cpu_mem = _DOCKER_STATS_CACHE.get(container)
                    if cpu_mem:
                        cpus[s.name], mems[s.name] = cpu_mem
                    else:
                        cpus[s.name] = "…"; mems[s.name] = "…"
                else:
                    uptimes[s.name] = "—"; cpus[s.name] = "—"; mems[s.name] = "—"
            else:
                pid = pids.get(s.name)
                if pid:
                    up, cpu, mem = proc_uptime_cpu_mem(pid)
                    uptimes[s.name] = up; cpus[s.name] = cpu; mems[s.name] = mem
                else:
                    uptimes[s.name] = "—"; cpus[s.name] = "—"; mems[s.name] = "—"
        # Kick a docker-stats refresh if the cache is stale; runs in
        # background so it never blocks the tick.
        if time.time() - _DOCKER_STATS_TS >= _DOCKER_STATS_TTL:
            self._refresh_docker_stats_worker()

        phases = {s.name: (read_svc_phase(s.name) or "") for s in SERVICES}
        new_state = LiveState(
            docker=docker_map,
            pids=pids,
            dirty=dict(self._cached_dirty),
            mtimes=dict(self._cached_mtimes),
            uptimes=uptimes,
            cpu_pct=cpus,
            mem_use=mems,
            phases=phases,
        )
        self.live = new_state
        self._refresh_table()
        self._update_status_bar()

    @work(exclusive=True, group="docker_stats")
    async def _refresh_docker_stats_worker(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _refresh_docker_stats)

    def _tick_banner(self) -> None:
        self._update_banner()

    def _tick_phase_animation(self) -> None:
        """Refresh only the STATE cells whose phase is mid-transition, so
        the trailing dots animate at 3 Hz without a full poll."""
        active = {
            name: phase for name, phase in self.live.phases.items() if phase
        }
        if not active:
            return
        table = self.query_one(DataTable)
        for name, phase in active.items():
            # Skip if the poller has already declared the service running —
            # don't fight the steady-state render.
            svc = SERVICES_BY_NAME.get(name)
            if svc is None:
                continue
            if svc.type == "docker":
                ds, dstatus = self.live.docker.get(name, ("", ""))
                if docker_state(ds, dstatus) == "running":
                    continue
            else:
                if self.live.pids.get(name):
                    continue
            with contextlib.suppress(Exception):
                table.update_cell(name, "state", self._phase_text(phase))

    @work(exclusive=True, group="log")
    async def _tick_log(self) -> None:
        if not REPL_LOG.exists():
            return
        try:
            size = REPL_LOG.stat().st_size
        except OSError:
            return
        if size < self.log_log_position:
            # File was truncated (new command).  Reset and reread tail.
            self.log_log_position = 0
            self.query_one("#log", RichLog).clear()
        if size == self.log_log_position:
            return
        with REPL_LOG.open("rb") as f:
            f.seek(self.log_log_position)
            new_bytes = f.read()
        self.log_log_position = size
        text = new_bytes.decode(errors="replace")
        log = self.query_one("#log", RichLog)
        for raw_line in text.splitlines():
            log.write(_strip_ansi_motion(raw_line))

    # ── Command handling ──
    def on_input_submitted(self, message: Input.Submitted) -> None:
        cmd = message.value.strip()
        message.input.value = ""
        if isinstance(message.input, HistoricInput) and cmd:
            message.input.push(cmd)
        if not cmd:
            return
        if cmd in ("q", "quit", "exit"):
            self.exit()
            return
        if cmd in ("h", "?", "help"):
            self._show_help()
            return
        if cmd in ("r", "refresh"):
            self.call_later(self._tick_state)
            return
        if cmd in ("clear",):
            self.query_one("#log", RichLog).clear()
            self._set_log_visible(False)
            return
        if cmd in ("log",):
            # Toggle log pane visibility manually.
            self._set_log_visible(not self._log_visible)
            return
        if cmd in ("banner",):
            self.action_toggle_banner()
            return
        self._dispatch(cmd)

    def _show_help(self) -> None:
        log = self.query_one("#log", RichLog)
        log.clear()
        self._set_log_visible(True)
        for line in [
            "Commands:",
            "  r           refresh state now",
            "  u           build + start every service",
            "  u <svc>     start one service (no rebuild)",
            "  d           stop every service",
            "  d <svc>     stop one service",
            "  b           force incremental sbt + node deps",
            "  a / auto    scan for dirty services and rebuild+bounce only those",
            "  <svc>       rebuild that service and bounce it",
            "  l <svc>     tail that service's log (Ctrl-C returns)",
            "  s <svc>     stop one service",
            "  clear       clear the log pane",
            "  log         toggle log pane visibility",
            "  banner      toggle banner (collapse the wordmark to save rows; Ctrl-B also works)",
            "  q           quit",
            "",
            "Mouse: double-click a service row → tail its log.",
            "       double-click the banner    → collapse / expand the wordmark.",
            "       Enter on a focused row does the same as double-click.",
            "",
            "BUILD column:",
            "  ★ (yellow)  source or deps changed since last build — rebuild needed",
            "  ↻ (cyan)    service auto-rebuilds on save (ng serve / bun --watch)",
            "  (blank)     built and up-to-date",
            "",
            f"Known services: {', '.join(s.name for s in SERVICES)}",
        ]:
            log.write(line)

    def _dispatch(self, cmd: str) -> None:
        if self._cmd_proc and self._cmd_proc.returncode is None:
            log = self.query_one("#log", RichLog)
            log.write(Text(f"busy: '{self.active_cmd}' still running. Ctrl-C in the term to abort.", style="bold yellow"))
            return

        parts = cmd.split(None, 1)
        verb = parts[0]
        arg = parts[1] if len(parts) > 1 else ""

        # Resolve to a bin/local-dev.sh invocation.  Keeping the shell script
        # as the canonical engine so behavior matches `bin/local-dev.sh up`
        # from a terminal.
        argv: Optional[list[str]] = None
        if verb in ("u", "up"):
            if arg:
                if arg not in SERVICES_BY_NAME:
                    self._log_err(f"unknown service: {arg}")
                    return
                argv = ["start", arg]
            else:
                argv = ["up"]
        elif verb in ("d", "down"):
            if arg:
                if arg not in SERVICES_BY_NAME:
                    self._log_err(f"unknown service: {arg}")
                    return
                argv = ["stop", arg]
            else:
                argv = ["down"]
        elif verb in ("s", "stop"):
            if not arg or arg not in SERVICES_BY_NAME:
                self._log_err("usage: s <service>")
                return
            argv = ["stop", arg]
        elif verb in ("b", "build"):
            # Force an incremental build; the shell handles the "is this
            # really needed" decision itself (it pre-bounces JVMs etc.).
            argv = ["up", "--build"]
        elif verb in ("a", "auto"):
            # Scan for dirty services and rebuild + bounce only those.
            argv = ["auto"]
        elif verb in ("l", "logs", "tail"):
            if not arg or arg not in SERVICES_BY_NAME:
                self._log_err(f"usage: l <service>  (known: {', '.join(s.name for s in SERVICES)})")
                return
            argv = ["logs", arg]
            self._spawn_logs(arg)
            return
        elif verb in SERVICES_BY_NAME:
            svc_obj = SERVICES_BY_NAME[verb]
            if svc_obj.type in WATCH_TYPES:
                # ng serve / bun --watch rebuild on save automatically. The
                # shell's cmd_update_one already refuses this, but we get a
                # nicer message by intercepting here.
                self._log_msg(
                    f"{verb} runs in watch mode (↻) — source edits auto-reload. "
                    f"If you really need to bounce it, run `s {verb}` then `u {verb}`."
                )
                return
            argv = [verb]
        else:
            self._log_err(f"unknown: {verb}   (type 'h' for help)")
            return

        if argv is None:
            return
        self._spawn_action(verb if not arg else f"{verb} {arg}", argv)

    def _log_err(self, msg: str) -> None:
        self._set_log_visible(True)
        self.query_one("#log", RichLog).write(Text("✗ " + msg, style="bold red"))

    def _log_msg(self, msg: str) -> None:
        self._set_log_visible(True)
        self.query_one("#log", RichLog).write(Text("• " + msg, style="cyan"))
        # Auto-dismiss the info pop after a few seconds.
        if self._log_auto_hide_handle is not None:
            with contextlib.suppress(Exception):
                self._log_auto_hide_handle.stop()
        self._log_auto_hide_handle = self.set_timer(4.0, self._auto_hide_log)

    def _spawn_logs(self, svc: str) -> None:
        log = self.query_one("#log", RichLog)
        log.clear()
        self._set_log_visible(True)
        log.write(Text(f"── tailing {svc} (type any other command to return) ──", style="dim"))
        self.active_cmd = f"logs {svc}"
        self.cmd_started_at = time.monotonic()
        self._tail_service_log(svc)

    @work(exclusive=True, group="cmd")
    async def _tail_service_log(self, svc: str) -> None:
        log_widget = self.query_one("#log", RichLog)
        svc_obj = SERVICES_BY_NAME[svc]
        if svc_obj.type == "docker":
            proc = await asyncio.create_subprocess_exec(
                "docker", "compose", "-p", DOCKER_PROJECT, "logs", "-f", svc,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
        else:
            log_path = LOG_DIR / f"{svc}.log"
            if not log_path.exists():
                log_path.touch()
            proc = await asyncio.create_subprocess_exec(
                "tail", "-n", "200", "-f", str(log_path),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            )
        self._cmd_proc = proc
        try:
            assert proc.stdout
            async for raw in proc.stdout:
                # If a newer action (e.g. `up`) has claimed _cmd_proc, our
                # exclusive-group cancellation is in flight but hasn't hit
                # the next await yet — stop writing to the widget *now* so
                # we don't leak frontend.log lines into the up output.
                if self._cmd_proc is not proc:
                    break
                log_widget.write(_strip_ansi_motion(raw.decode(errors="replace").rstrip("\n")))
        finally:
            # ESC fires _cancel_active_cmd which terminates this proc AND
            # immediately clears active_cmd/_cmd_proc so the user can run
            # another command. By the time our async-for unwinds the proc
            # we're holding, the user may have already kicked off `up`. Only
            # clear the global state if it still belongs to us — otherwise
            # we'd clobber a freshly-spawned command's state to idle, and
            # any subsequent up/down would see a stale view.
            if self._cmd_proc is proc:
                self.active_cmd = None
                self._cmd_proc = None

    @work(exclusive=True, group="cmd")
    async def _spawn_action(self, label: str, argv: list[str]) -> None:
        log = self.query_one("#log", RichLog)
        # Cancel any pending auto-hide from a previous command.
        if self._log_auto_hide_handle is not None:
            with contextlib.suppress(Exception):
                self._log_auto_hide_handle.stop()
            self._log_auto_hide_handle = None
        # Trip _tail_service_log's ownership check (`self._cmd_proc is not
        # proc → break`) immediately. Otherwise the tail keeps draining
        # frontend.log into the log widget across our log.clear() + the
        # async point at create_subprocess_exec below, and the user sees
        # frontend output mixed into the up panel.
        self._cmd_proc = None
        # Truncate REPL_LOG so the log pane only shows this command's output.
        REPL_LOG.write_text("")
        log.clear()
        self._set_log_visible(True)
        log.write(Text(f"── {label}  →  bin/local-dev.sh {' '.join(shlex.quote(a) for a in argv)} ──", style="dim"))
        self.active_cmd = label
        self.cmd_started_at = time.monotonic()
        self.log_log_position = 0

        with REPL_LOG.open("wb") as out:
            proc = await asyncio.create_subprocess_exec(
                str(LOCAL_DEV_SH), *argv,
                stdout=out, stderr=asyncio.subprocess.STDOUT,
                cwd=str(REPO_ROOT),
            )
            self._cmd_proc = proc
            await proc.wait()
        rc = proc.returncode or 0
        style = "bold green" if rc == 0 else "bold red"
        log.write(Text(f"── {label}: done (exit {rc}) ──", style=style))
        # Same guard as _tail_service_log: only release the global state if
        # it still points at our proc. Otherwise a faster newer command has
        # already taken over and we mustn't reset it to idle.
        if self._cmd_proc is proc:
            self.active_cmd = None
            self._cmd_proc = None
        # Right after a command, source state likely moved — force a state poll.
        self._last_dirty_check = 0
        self.call_later(self._tick_state)

        # Successful command → auto-hide the log so the dashboard reclaims the
        # space.  Failure stays visible so the user can read the error.
        if rc == 0:
            self._log_auto_hide_handle = self.set_timer(3.0, self._auto_hide_log)

    def _auto_hide_log(self) -> None:
        # Only hide if no new command came in and the user hasn't engaged with
        # the log (we keep it pinned during interactive log tailing).
        if self.active_cmd is None:
            self._set_log_visible(False)
        self._log_auto_hide_handle = None

    def action_toggle_banner(self) -> None:
        """Collapse / expand the ASCII wordmark to reclaim ~7 rows."""
        banner = self.query_one("#banner")
        if banner.has_class("-collapsed"):
            banner.remove_class("-collapsed")
        else:
            banner.add_class("-collapsed")

    def action_clear_log(self) -> None:
        self.query_one("#log", RichLog).clear()
        self._set_log_visible(False)

    def action_manual_refresh(self) -> None:
        self.call_later(self._tick_state)

    # ── ESC: leave whatever transient view we're in ──
    def action_escape_view(self) -> None:
        # If a log tail (or any running command) is up, cancel it.
        if self.active_cmd is not None:
            self._cancel_active_cmd()
        # Hide the log pane regardless — ESC's main job is to give the
        # dashboard the screen back.
        if self._log_visible:
            self._set_log_visible(False)
        # Make sure the prompt has focus so the user can immediately type.
        self.query_one("#prompt", HistoricInput).focus()

    # ── Ctrl-C: cancel current work, or quit on a second tap ──
    def action_soft_quit(self) -> None:
        # 1. Active command (build / up / log tail …) → kill it, keep the
        #    REPL open.
        if self.active_cmd is not None:
            self._cancel_active_cmd()
            self.notify("Ctrl-C — cancelled current command", timeout=2)
            self._last_ctrl_c_ts = 0.0
            return
        # 2. Idle: require a second Ctrl-C within 2 s to actually exit. This
        #    prevents an accidental ⌃-C from killing a session you wanted to
        #    keep.
        now = time.monotonic()
        if now - self._last_ctrl_c_ts < 2.0:
            self.exit()
            return
        self._last_ctrl_c_ts = now
        self.notify("Press Ctrl-C again within 2 s to quit", timeout=2)

    def _cancel_active_cmd(self) -> None:
        proc = self._cmd_proc
        if proc and proc.returncode is None:
            with contextlib.suppress(ProcessLookupError, OSError):
                proc.terminate()
        # Cancel the worker too. proc.terminate() races with stdout draining
        # — until the worker's `async for` actually returns, Textual still
        # considers it alive in group "cmd", which can block the next
        # `@work(exclusive=True, group="cmd")` from ever starting. That's
        # the bug behind "tap into a log → ESC → `down` does nothing".
        with contextlib.suppress(Exception):
            self.workers.cancel_group(self, "cmd")
        self.active_cmd = None
        self._cmd_proc = None

    # ── Mouse: double-click a service row to tail its log ──
    def on_click(self, event: events.Click) -> None:
        widget = getattr(event, "control", None) or getattr(event, "widget", None)

        # Double-click on any banner element → toggle (collapse / expand).
        # Lets the user reclaim the wordmark's rows without remembering the
        # Ctrl-B / `banner` aliases.
        if event.chain == 2 and self._widget_in_banner(widget):
            self.action_toggle_banner()
            return

        # Double-click on a service row → tail that service's log.
        if event.chain == 2:
            table = self.query_one(DataTable)
            if widget is table:
                row = table.cursor_row
                if 0 <= row < len(SERVICES):
                    self._spawn_logs(SERVICES[row].name)
                    self.query_one("#prompt", Input).focus()

    def _widget_in_banner(self, widget) -> bool:
        """True when `widget` is the banner container or any of its
        children. We walk the parent chain rather than matching IDs so a
        click on the wordmark / title / sub line all count."""
        node = widget
        while node is not None:
            if getattr(node, "id", None) == "banner":
                return True
            node = getattr(node, "parent", None)
        return False

    # Enter on a focused row works too (keyboard equivalent of double-click).
    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        svc = str(event.row_key.value) if event.row_key.value else ""
        if svc and svc in SERVICES_BY_NAME:
            self._spawn_logs(svc)


# ─────────────────── ANSI motion stripper for log ───────────────────

_CSI_NON_SGR = re.compile(r"\x1b\[[0-9;?]*[A-LN-Za-ln-z]")   # everything except SGR (m)
_CR = re.compile(r"\r+")


def _strip_ansi_motion(s: str) -> str:
    """Strip cursor-motion / erase-screen CSI sequences while keeping SGR colors,
    and collapse \\r so spinner frames don't pile up in the log pane."""
    s = _CSI_NON_SGR.sub("", s)
    s = _CR.sub("", s)
    return s


# ─────────────────── Entry point ───────────────────

def main() -> None:
    if not LOCAL_DEV_SH.exists():
        print(f"FATAL: {LOCAL_DEV_SH} not found", file=__import__("sys").stderr)
        raise SystemExit(1)
    if shutil.which("docker") is None:
        print("FATAL: docker not on PATH", file=__import__("sys").stderr)
        raise SystemExit(1)
    LocalDevApp().run()


if __name__ == "__main__":
    main()
