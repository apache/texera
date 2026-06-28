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

"""bin/local-dev-tui.py — Textual dashboard for the Texera local dev stack.

Lives next to bin/local-dev.sh; the shell script remains the canonical engine
(build, start, stop, status) and this TUI shells out to it for every action.
The dashboard itself owns state polling, dirty-source detection, and the
prompt loop.  Textual handles diff rendering so the screen doesn't accrete in
scrollback the way the old zsh `\\e[H` redraw did.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import os
import re
import shlex
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime
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

REPO_ROOT = Path(__file__).resolve().parent.parent
STATE_DIR = Path(os.environ.get("TEXERA_LOCAL_DEV_DIR", "/tmp/texera-local-dev"))
LOG_DIR = STATE_DIR / "logs"
BUILD_STAMP_DIR = STATE_DIR / "build-stamps"
REPL_LOG = LOG_DIR / "repl.log"
LOG_DIR.mkdir(parents=True, exist_ok=True)
BUILD_STAMP_DIR.mkdir(parents=True, exist_ok=True)

LOCAL_DEV_SH = REPO_ROOT / "bin" / "local-dev.sh"
DOCKER_PROJECT = "texera-local-dev"

HISTORY_FILE = STATE_DIR / "tui-history"
MAX_HISTORY = 500

COMMON_SRC = [
    "common/dao/src",
    "common/config/src",
    "common/auth/src",
    "common/workflow-core/src",
    "common/workflow-operator/src",
    "common/pybuilder/src",
]
SOURCE_SUFFIXES = {".scala", ".java", ".proto"}

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
    if bs.exists():
        m = _VERSION_RE.search(bs.read_text(errors="replace"))
        if m:
            return m.group(1)
    return "1.3.0-incubating-SNAPSHOT"   # last-ditch fallback


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


def _jvm(name: str, port: int, project: str, own_src: str) -> Service:
    """sbt-native-packager lays the dist out as
    `target/<artifact>-<VERSION>/lib/org.apache.texera.<artifact>-<VERSION>.jar`
    for every subproject. The single exception is amber: its sbt subproject
    name is `amber` (not `texera-web`) and the dist goes under `amber/target/`
    rather than the repo-level `target/`."""
    is_amber = name == "texera-web"
    artifact = "amber" if is_amber else name
    target_prefix = "amber/" if is_amber else ""
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
    _jvm("computing-unit-managing-service", 8082, "ComputingUnitManagingService",
         "computing-unit-managing-service/src"),
    _jvm("texera-web",                      8080, "WorkflowExecutionService",
         "amber/src"),
    Service("agent-service", "bun",  3001),
    Service("frontend",      "yarn", 4200),
]

SERVICES_BY_NAME = {s.name: s for s in SERVICES}


# ─────────────────── Live state model ───────────────────

@dataclass
class LiveState:
    """Snapshot of the world this tick — what the dashboard renders."""
    docker: dict[str, tuple[str, str]] = field(default_factory=dict)   # name -> (state, status)
    pids: dict[str, Optional[str]] = field(default_factory=dict)       # name -> pid or None
    dirty: dict[str, bool] = field(default_factory=dict)
    mtimes: dict[str, Optional[str]] = field(default_factory=dict)


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

def _jvm_src_dirs(svc: Service) -> list[Path]:
    dirs = [REPO_ROOT / d for d in COMMON_SRC]
    if svc.own_src:
        dirs.insert(0, REPO_ROOT / svc.own_src)
    return [d for d in dirs if d.exists()]


def _jvm_source_files(svc: Service) -> list[Path]:
    files: list[Path] = []
    for d in _jvm_src_dirs(svc):
        for f in d.rglob("*"):
            if f.is_file() and f.suffix in SOURCE_SUFFIXES:
                files.append(f)
    files.sort()
    return files


def source_hash(svc: Service) -> str:
    h = hashlib.sha1()
    for f in _jvm_source_files(svc):
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
        if not nm.exists():
            return True
        return lock.stat().st_mtime > nm.stat().st_mtime
    if svc.type == "bun":
        nm = REPO_ROOT / "agent-service" / "node_modules"
        lock = REPO_ROOT / "agent-service" / "bun.lock"
        if not nm.exists():
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

    # Slow path — did the content actually move?
    stored = stamp.read_text().strip()
    current = source_hash(svc)
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
        height: 3;
        background: $boost;
        color: $text;
        padding: 0 2;
        border-bottom: heavy $primary;
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
        padding: 0 2;
    }
    #log-header.-hidden { display: none; }
    RichLog {
        height: 12;
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
        header = self.query_one("#log-header", Static)
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
    _COL_LABELS = ("●", "SERVICE", "PORT", "PID", "ARTIFACT", "BUILD", "STATE")
    _COL_KEYS   = ("sym", "svc",     "port", "pid", "mtime",    "src",   "state")

    # ── Layout ──
    def compose(self) -> ComposeResult:
        yield Vertical(
            Static("", id="banner-title"),
            Static("", id="banner-sub"),
            id="banner",
        )
        table = DataTable(zebra_stripes=False, cursor_type="row")
        for label, key in zip(self._COL_LABELS, self._COL_KEYS):
            table.add_column(label, key=key)
        for s in SERVICES:
            table.add_row("○", s.name, f":{s.port}", "—", "—", "  ", "stopped", key=s.name)
        yield table
        yield Static("log:", id="log-header", classes="-hidden")
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
        # Kick the first poll immediately so the table populates fast.
        self.call_later(self._tick_state)
        self.call_later(self._tick_log)

    # ── Updates ──
    def _update_banner(self) -> None:
        now = datetime.now().strftime("%H:%M:%S")
        wt_tag = f"worktree: {self._worktree_name}" if self._is_worktree else f"checkout: {self._worktree_name}"
        sub = f"{wt_tag}  ·  branch: {self._branch} @ {self._sha}  ·  {now}"
        self.query_one("#banner-title", Static).update("Texera Local Dev — interactive")
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
        self.query_one("#status-bar", Static).update(
            f"{running}/{total} running{dirty_part}    last: {active}{elapsed}"
        )

    def _refresh_table(self) -> None:
        table = self.query_one(DataTable)
        for svc in SERVICES:
            if svc.type == "docker":
                ds, dstatus = self.live.docker.get(svc.name, ("", ""))
                state = docker_state(ds, dstatus)
                pid = "—"
            else:
                pid = self.live.pids.get(svc.name) or "—"
                state = "running" if self.live.pids.get(svc.name) else "stopped"
            sym, style = STATE_STYLE.get(state, ("○", "grey50"))
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

            table.update_cell(svc.name, "sym",   Text(sym, style=style))
            table.update_cell(svc.name, "port",  f":{svc.port}")
            table.update_cell(svc.name, "pid",   str(pid))
            table.update_cell(svc.name, "mtime", mtime)
            table.update_cell(svc.name, "src",   src_cell)
            table.update_cell(svc.name, "state", Text(state, style=style))

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

        new_state = LiveState(
            docker=docker_map,
            pids=pids,
            dirty=dict(self._cached_dirty),
            mtimes=dict(self._cached_mtimes),
        )
        self.live = new_state
        self._refresh_table()
        self._update_status_bar()

    def _tick_banner(self) -> None:
        self._update_banner()

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
            "  <svc>       rebuild that service and bounce it",
            "  l <svc>     tail that service's log (Ctrl-C returns)",
            "  s <svc>     stop one service",
            "  clear       clear the log pane",
            "  log         toggle log pane visibility",
            "  q           quit",
            "",
            "Mouse: double-click a service row to tail its log.",
            "       Enter on a focused row does the same.",
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
                log_widget.write(_strip_ansi_motion(raw.decode(errors="replace").rstrip("\n")))
        finally:
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
        self.active_cmd = None
        self._cmd_proc = None

    # ── Mouse: double-click a service row to tail its log ──
    def on_click(self, event: events.Click) -> None:
        if event.chain != 2:
            return
        table = self.query_one(DataTable)
        # If the click didn't land on the table widget itself, ignore.
        widget = getattr(event, "control", None) or getattr(event, "widget", None)
        if widget is not table:
            return
        row = table.cursor_row
        if 0 <= row < len(SERVICES):
            self._spawn_logs(SERVICES[row].name)
            # Keep typing focused on the prompt — clicking the table shouldn't
            # eat further keystrokes.
            self.query_one("#prompt", Input).focus()

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
