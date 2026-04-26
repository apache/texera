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

from threading import Condition

import pytest

from core.architecture.managers.debug_manager import DebugManager


class TestDebugManager:
    @pytest.fixture
    def debug_manager(self):
        return DebugManager(Condition())

    def test_it_can_init(self, debug_manager):
        assert debug_manager.debugger is not None
        assert debug_manager.debugger.prompt == ""

    def test_it_has_no_command_initially(self, debug_manager):
        assert not debug_manager.has_debug_command()

    def test_it_has_no_event_initially(self, debug_manager):
        assert not debug_manager.has_debug_event()

    def test_put_command_sets_has_debug_command(self, debug_manager):
        debug_manager.put_debug_command("n")
        assert debug_manager.has_debug_command()

    def test_get_debug_event_returns_flushed_output(self, debug_manager):
        # Pdb writes to its stdout via the SingleBlockingIO; simulate that path
        # directly so we don't have to spin up a real debugging session.
        debug_manager.debugger.stdout.write("hit breakpoint")
        debug_manager.debugger.stdout.flush()
        assert debug_manager.has_debug_event()
        assert debug_manager.get_debug_event() == "hit breakpoint\n"
        assert not debug_manager.has_debug_event()

    def test_command_pipe_and_event_pipe_are_independent(self, debug_manager):
        debug_manager.put_debug_command("step")
        assert debug_manager.has_debug_command()
        assert not debug_manager.has_debug_event()

        debug_manager.debugger.stdout.write("event")
        debug_manager.debugger.stdout.flush()
        # Putting a command must not consume an event, and vice versa.
        assert debug_manager.has_debug_command()
        assert debug_manager.has_debug_event()

    def test_pdb_is_wired_to_debug_pipes(self, debug_manager):
        # The Pdb instance must read from the same IO that put_debug_command
        # writes to, and write to the same IO that get_debug_event reads from.
        debug_manager.put_debug_command("c")
        # Reading via the debugger's stdin must see the queued command.
        assert debug_manager.debugger.stdin.readline() == "c\n"

        debug_manager.debugger.stdout.write("paused")
        debug_manager.debugger.stdout.flush()
        assert debug_manager.get_debug_event() == "paused\n"

    def test_event_pipe_supports_multiple_round_trips(self, debug_manager):
        for line in ("first", "second", "third"):
            debug_manager.debugger.stdout.write(line)
            debug_manager.debugger.stdout.flush()
            assert debug_manager.get_debug_event() == f"{line}\n"
            assert not debug_manager.has_debug_event()

    def test_debugger_uses_nosigint_to_avoid_signal_install(self, debug_manager):
        # We construct Pdb with nosigint=True to avoid touching signal handlers
        # in the worker thread. Guard against accidental flips.
        assert debug_manager.debugger.nosigint is True

    # ----- edge cases / quirks -----

    def test_put_empty_command_still_marks_command_present(self, debug_manager):
        # SingleBlockingIO.flush always commits buf + "\n" to value, so even
        # an empty command becomes a "\n" line and shows up as a pending
        # command. Documents current behavior.
        debug_manager.put_debug_command("")
        assert debug_manager.has_debug_command()
        assert debug_manager.debugger.stdin.readline() == "\n"

    def test_put_overwrites_unconsumed_command(self, debug_manager):
        # The command pipe holds at most one value. A second put without an
        # intervening consume silently overwrites the first — known data-loss
        # quirk of SingleBlockingIO. Pinning this so callers don't accidentally
        # rely on queued semantics.
        debug_manager.put_debug_command("first")
        debug_manager.put_debug_command("second")
        assert debug_manager.debugger.stdin.readline() == "second\n"

    def test_put_command_with_embedded_newline_is_passed_verbatim(self, debug_manager):
        # An embedded newline is not sanitized; pdb would see the raw bytes.
        debug_manager.put_debug_command("step\nlist")
        assert debug_manager.debugger.stdin.readline() == "step\nlist\n"

    def test_event_pipe_overwrites_unconsumed_event(self, debug_manager):
        debug_manager.debugger.stdout.write("first")
        debug_manager.debugger.stdout.flush()
        debug_manager.debugger.stdout.write("second")
        debug_manager.debugger.stdout.flush()
        assert debug_manager.get_debug_event() == "second\n"
