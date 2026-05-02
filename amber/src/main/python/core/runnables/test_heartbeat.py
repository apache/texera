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
from unittest.mock import patch, MagicMock

import pytest

from core.runnables.heartbeat import Heartbeat


def make_heartbeat(host="localhost", port=12345, interval=0.05, event=None):
    return Heartbeat(host, port, interval, event or Event())


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

    def test_returns_false_when_socket_close_raises(self):
        # Pins the false-negative path: connect succeeds but the subsequent
        # close() throws (e.g. broken pipe on a half-open socket). The bare
        # `except Exception` in _check_heartbeat() catches it and reports
        # "server down", and a regression that narrows that handler would be
        # caught here.
        hb = make_heartbeat()
        fake_sock = MagicMock()
        fake_sock.close.side_effect = OSError("close failed")
        with patch(
            "core.runnables.heartbeat.socket.create_connection",
            return_value=fake_sock,
        ):
            assert hb._check_heartbeat() is False


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
