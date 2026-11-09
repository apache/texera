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

import socket
from threading import Event
from types import SimpleNamespace
from unittest.mock import call, patch, MagicMock

import pytest

from core.runnables.heartbeat import Heartbeat


def make_heartbeat(host="localhost", port=12345, interval=0.05, event=None):
    return Heartbeat(host, port, interval, event or Event())


class ScriptedEvent:
    """A drop-in for ``threading.Event`` whose ``wait`` returns a scripted
    sequence instead of sleeping.

    ``False`` means "the timeout elapsed, keep looping" and ``True`` means
    "the stop event fired, leave the loop", which is exactly how
    ``Heartbeat.run`` reads the return value. Driving the loop this way keeps
    the tests deterministic and instantaneous -- no real sleeps -- while still
    recording the timeout each call was made with, so the interval wiring
    stays under test. Once the script is exhausted it returns ``True`` so a
    regression can never spin forever.
    """

    def __init__(self, results):
        self._results = list(results)
        self.wait_timeouts = []

    def wait(self, timeout=None):
        self.wait_timeouts.append(timeout)
        if not self._results:
            return True
        return self._results.pop(0)


# ``signal.SIGKILL`` does not exist on Windows, so `Heartbeat.stop` is pinned
# against these stand-ins: the assertions then check that the kill signals are
# read off the `signal` module rather than hard-coded, on every platform.
FAKE_SIGNALS = SimpleNamespace(SIGKILL="SIGKILL", SIGTERM="SIGTERM")


class TestHeartbeatInit:
    def test_parses_host_and_port_from_grpc_tcp_url(self):
        hb = make_heartbeat(host="example.test", port=9090)
        assert hb._parsed_server_host == "example.test"
        assert hb._parsed_server_port == 9090

    def test_records_interval_and_stop_event_references(self):
        event = Event()
        hb = make_heartbeat(interval=2.5, event=event)
        assert hb._interval == 2.5
        assert hb._stop_event is event

    def test_captures_original_parent_pid_at_construction_time(self):
        with patch("core.runnables.heartbeat.os.getppid", return_value=4242):
            hb = make_heartbeat()
        assert hb._original_parent_pid == 4242

    def test_supports_ipv6_host_in_bracketed_form(self):
        hb = make_heartbeat(host="[::1]", port=9090)
        assert hb._parsed_server_host == "::1"
        assert hb._parsed_server_port == 9090


class TestCheckHeartbeat:
    def test_returns_true_when_socket_connects(self):
        hb = make_heartbeat(host="h", port=1)
        fake_sock = MagicMock()
        with patch(
            "core.runnables.heartbeat.socket.create_connection",
            return_value=fake_sock,
        ) as mock_connect:
            assert hb._check_heartbeat() is True
            mock_connect.assert_called_once_with(("h", 1), timeout=1)
            fake_sock.close.assert_called_once()

    def test_returns_false_when_socket_connection_raises(self):
        hb = make_heartbeat()
        with patch(
            "core.runnables.heartbeat.socket.create_connection",
            side_effect=ConnectionRefusedError("nope"),
        ):
            assert hb._check_heartbeat() is False

    def test_returns_false_when_socket_connection_times_out(self):
        hb = make_heartbeat()
        with patch(
            "core.runnables.heartbeat.socket.create_connection",
            side_effect=socket.timeout("slow"),
        ):
            assert hb._check_heartbeat() is False

    def test_returns_true_when_connection_succeeds_but_socket_close_raises(self):
        hb = make_heartbeat()
        fake_sock = MagicMock()
        fake_sock.close.side_effect = OSError("close failed")
        with patch(
            "core.runnables.heartbeat.socket.create_connection",
            return_value=fake_sock,
        ):
            assert hb._check_heartbeat() is True
            fake_sock.close.assert_called_once()


class TestRunEarlyExit:
    @pytest.mark.timeout(2)
    def test_returns_immediately_when_stop_event_is_already_set(self):
        event = Event()
        event.set()
        hb = make_heartbeat(interval=10.0, event=event)
        # Event.wait(timeout=10) returns immediately because the event is
        # already set, so `while not self._stop_event.wait(...)` short-circuits
        # before the loop body runs and _check_heartbeat() is never called.
        # The pytest timeout above turns a regression that re-enters the loop
        # (or blocks on wait()) into a fast failure rather than a hung CI job.
        with patch.object(hb, "_check_heartbeat") as mock_check:
            hb.run()
        mock_check.assert_not_called()


@pytest.mark.parametrize("port", [1, 65535, 8080])
def test_init_accepts_full_port_range(port):
    hb = make_heartbeat(port=port)
    assert hb._parsed_server_port == port


class TestRunLoop:
    @pytest.mark.timeout(2)
    def test_polls_once_per_interval_while_the_server_stays_up(self):
        event = ScriptedEvent([False, False, True])
        hb = make_heartbeat(interval=0.25, event=event)
        with (
            patch.object(hb, "_check_heartbeat", return_value=True) as mock_check,
            patch.object(hb, "stop") as mock_stop,
        ):
            hb.run()

        # one probe per elapsed interval, and none after the stop event fired
        assert mock_check.call_count == 2
        assert event.wait_timeouts == [0.25, 0.25, 0.25]
        mock_stop.assert_not_called()

    @pytest.mark.timeout(2)
    def test_a_single_failed_probe_is_not_enough_to_stop(self):
        event = ScriptedEvent([False, False, True])
        hb = make_heartbeat(interval=0.05, event=event)
        # first probe fails, the double check succeeds -> treated as a blip
        with (
            patch.object(
                hb, "_check_heartbeat", side_effect=[False, True, True]
            ) as mock_check,
            patch.object(hb, "stop") as mock_stop,
        ):
            hb.run()

        assert mock_check.call_count == 3
        mock_stop.assert_not_called()

    @pytest.mark.timeout(2)
    def test_two_failed_probes_stop_the_worker_and_end_the_loop(self):
        event = ScriptedEvent([False, False, False])
        hb = make_heartbeat(interval=0.05, event=event)
        with (
            patch.object(
                hb, "_check_heartbeat", side_effect=[False, False]
            ) as mock_check,
            patch.object(hb, "stop") as mock_stop,
        ):
            hb.run()

        assert mock_check.call_count == 2
        mock_stop.assert_called_once_with()
        # run() returned instead of waiting again, even though the stop event
        # was never set
        assert event.wait_timeouts == [0.05]

    @pytest.mark.timeout(2)
    def test_logs_the_unchanged_parent_pid_and_its_status_before_stopping(self):
        with patch("core.runnables.heartbeat.os.getppid", return_value=4242):
            hb = make_heartbeat(interval=0.05, event=ScriptedEvent([False]))

        with (
            patch("core.runnables.heartbeat.os.getppid", return_value=4242),
            patch("core.runnables.heartbeat.psutil.Process") as mock_process,
            patch("core.runnables.heartbeat.logger") as mock_logger,
            patch.object(hb, "_check_heartbeat", return_value=False),
            patch.object(hb, "stop"),
        ):
            mock_process.return_value.status.return_value = "zombie"
            hb.run()

        mock_process.assert_called_once_with(4242)
        message = mock_logger.warning.call_args[0][0]
        assert "Parent process PID 4242 runs unusually." in message
        assert "Parent PID hasn't changed." in message
        assert "Original parent process Status: zombie" in message

    @pytest.mark.timeout(2)
    def test_reports_a_reparented_and_vanished_parent_process(self):
        with patch("core.runnables.heartbeat.os.getppid", return_value=4242):
            hb = make_heartbeat(interval=0.05, event=ScriptedEvent([False]))

        with (
            # the JVM died and this worker was reparented to init
            patch("core.runnables.heartbeat.os.getppid", return_value=1),
            patch(
                "core.runnables.heartbeat.psutil.Process",
                side_effect=RuntimeError("no such process"),
            ),
            patch("core.runnables.heartbeat.logger") as mock_logger,
            patch.object(hb, "_check_heartbeat", return_value=False),
            patch.object(hb, "stop") as mock_stop,
        ):
            hb.run()

        message = mock_logger.warning.call_args[0][0]
        assert "Parent PID changed to 1." in message
        # a lookup failure degrades to a placeholder status instead of
        # bubbling out of run()
        assert "Original parent process Status: NOT FOUND" in message
        mock_stop.assert_called_once_with()


class TestStop:
    @staticmethod
    def fake_child(pid, running=True):
        child = MagicMock(name=f"child-{pid}")
        child.pid = pid
        child.is_running.return_value = running
        return child

    def test_kills_running_descendants_then_terminates_itself(self):
        hb = make_heartbeat()
        running_child = self.fake_child(111)
        dead_child = self.fake_child(222, running=False)

        with (
            patch("core.runnables.heartbeat.psutil.Process") as mock_process,
            patch("core.runnables.heartbeat.os.kill") as mock_kill,
            patch("core.runnables.heartbeat.os.getpid", return_value=777),
            patch("core.runnables.heartbeat.signal", FAKE_SIGNALS),
        ):
            mock_process.return_value.children.return_value = [
                running_child,
                dead_child,
            ]
            hb.stop()

        mock_process.return_value.children.assert_called_once_with(recursive=True)
        # the already-dead child is skipped; self-termination comes last
        assert mock_kill.call_args_list == [
            call(111, "SIGKILL"),
            call(777, "SIGTERM"),
        ]

    def test_terminates_itself_even_without_any_children(self):
        hb = make_heartbeat()
        with (
            patch("core.runnables.heartbeat.psutil.Process") as mock_process,
            patch("core.runnables.heartbeat.os.kill") as mock_kill,
            patch("core.runnables.heartbeat.os.getpid", return_value=777),
            patch("core.runnables.heartbeat.signal", FAKE_SIGNALS),
        ):
            mock_process.return_value.children.return_value = []
            hb.stop()

        assert mock_kill.call_args_list == [call(777, "SIGTERM")]

    def test_a_failing_child_kill_does_not_abort_the_cleanup(self):
        hb = make_heartbeat()
        first_child = self.fake_child(111)
        second_child = self.fake_child(222)

        with (
            patch("core.runnables.heartbeat.psutil.Process") as mock_process,
            patch("core.runnables.heartbeat.os.kill") as mock_kill,
            patch("core.runnables.heartbeat.os.getpid", return_value=777),
            patch("core.runnables.heartbeat.signal", FAKE_SIGNALS),
            patch("core.runnables.heartbeat.logger") as mock_logger,
        ):
            mock_process.return_value.children.return_value = [
                first_child,
                second_child,
            ]
            mock_kill.side_effect = [ProcessLookupError("gone"), None, None]
            hb.stop()

        assert mock_kill.call_args_list == [
            call(111, "SIGKILL"),
            call(222, "SIGKILL"),
            call(777, "SIGTERM"),
        ]
        warning = mock_logger.warning.call_args[0][0]
        assert "PID 111" in warning
        assert "gone" in warning
