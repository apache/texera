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

import threading
from unittest.mock import MagicMock, patch

import pytest
from pyarrow import Table
from pyarrow.flight import Action

from core.proxy.proxy_server import ProxyServer, get_free_local_port


class TestProxyServer:
    @pytest.fixture()
    def server(self):
        server = ProxyServer()
        yield server
        server.graceful_shutdown()

    def test_server_can_register_control_actions_with_lambda(self, server):
        assert "hello" not in server._procedures
        server.register("hello", lambda: None)
        assert "hello" in server._procedures

    def test_server_can_register_control_actions_with_function(self, server):
        def hello():
            return None

        assert "hello" not in server._procedures
        server.register("hello", hello)
        assert "hello" in server._procedures

    def test_server_can_register_control_actions_with_callable_class(self, server):
        class Hello:
            def __call__(self):
                return None

        assert "hello" not in server._procedures
        server.register("hello", Hello())
        assert "hello" in server._procedures

    def test_server_can_invoke_registered_control_actions(self, server):
        procedure_contents = {
            "hello": "hello world",
            "get an int": 12,
            "get a float": 1.23,
            "get a tuple": (5, None, 123.4),
            "get a list": [5, (None, 123.4)],
            "get a dict": {"entry": [5, (None, 123.4)]},
        }

        for name, result in procedure_contents.items():
            server.register(name, lambda: result)
            assert name in server._procedures
            assert next(
                server.do_action(None, Action(name, b""))
            ).body.to_pybytes() == str(result).encode("utf-8")

    def test_shutdown_action_yields_reply_before_starting_shutdown(self, server):
        shutdown_started = threading.Event()
        with patch.object(
            server, "graceful_shutdown", side_effect=shutdown_started.set
        ) as mock_shutdown:
            results = server.do_action(None, Action("shutdown", b""))

            first = next(results)
            assert first.body.to_pybytes() == b"Bye bye!"
            assert not mock_shutdown.called

            with pytest.raises(StopIteration):
                next(results)
            assert shutdown_started.wait(timeout=5)
            mock_shutdown.assert_called_once()

    @staticmethod
    def do_put_args(command: bytes = b"cmd", data: Table = None):
        """Build the (descriptor, reader, writer) triple that Flight hands to
        do_put. Only the three attributes do_put touches are stubbed."""
        if data is None:
            data = Table.from_pydict({"x": [1, 2]})
        descriptor = MagicMock()
        descriptor.command = command
        reader = MagicMock()
        reader.read_all.return_value = data
        return descriptor, reader, MagicMock()

    def test_heartbeat_is_registered_and_acks(self, server):
        # The default liveness action the client polls before sending work.
        assert "heartbeat" in server._procedures
        result = next(server.do_action(None, Action("heartbeat", b"")))
        assert result.body.to_pybytes() == b"ack"

    def test_unknown_action_raises_key_error(self, server):
        with pytest.raises(KeyError, match="Unknown action"):
            next(server.do_action(None, Action("no-such-action", b"")))

    def test_registering_the_same_name_overwrites_the_previous_action(self, server):
        server.register("hello", lambda: "first")
        server.register("hello", lambda: "second")

        assert len(server._procedures) == 5  # 4 defaults + "hello", not 6
        result = next(server.do_action(None, Action("hello", b"")))
        assert result.body.to_pybytes() == b"second"

    def test_action_returning_bytes_is_passed_through_unencoded(self, server):
        # bytes results are forwarded verbatim; everything else is str()-ed.
        server.register("raw", lambda: b"\x00\x01raw")

        result = next(server.do_action(None, Action("raw", b"")))
        assert result.body.to_pybytes() == b"\x00\x01raw"

    def test_action_receives_the_body_as_payload_when_non_empty(self, server):
        seen = []
        server.register("echo", lambda payload: seen.append(payload) or payload)

        result = next(server.do_action(None, Action("echo", b"payload")))
        assert seen == [b"payload"]
        assert result.body.to_pybytes() == b"payload"

    def test_error_inside_an_action_is_reraised_to_the_caller(self, server):
        # register() wraps the action in a logging catcher, but with
        # reraise=True so the failure still surfaces on the client call.
        def boom():
            raise ValueError("action failed")

        server.register("boom", boom)
        with pytest.raises(ValueError, match="action failed"):
            next(server.do_action(None, Action("boom", b"")))

    def test_list_actions_reports_names_with_descriptions(self, server):
        server.register("hello", lambda: None, description="Say hello.")

        listed = dict(server.list_actions(None))
        assert listed["hello"] == "Say hello."
        assert listed["shutdown"] == "Shut down this server."
        assert listed["control"] == "Process the control message"
        assert listed["actor"] == "Process the actor message"
        # heartbeat is registered without a description.
        assert listed["heartbeat"] == ""


class TestProxyServerHandlers:
    """Handler registration and the data path, which the default constructor
    leaves unimplemented until the runtime wires them up."""

    @pytest.fixture()
    def server(self):
        server = ProxyServer()
        yield server
        server.graceful_shutdown()

    def test_data_handler_must_accept_command_and_data(self, server):
        with pytest.raises(AssertionError):
            server.register_data_handler(lambda only_one: None)

    def test_control_handler_must_accept_at_least_one_argument(self, server):
        with pytest.raises(AssertionError):
            server.register_control_handler(lambda: None)

    def test_do_put_without_a_registered_handler_is_not_implemented(self, server):
        descriptor, reader, writer = TestProxyServer.do_put_args()
        with pytest.raises(NotImplementedError):
            server.do_put(None, descriptor, reader, writer)

    def test_control_action_without_a_registered_handler_is_not_implemented(
        self, server
    ):
        with pytest.raises(NotImplementedError):
            next(server.do_action(None, Action("control", b"payload")))

    def test_actor_action_without_a_registered_handler_is_not_implemented(self, server):
        with pytest.raises(NotImplementedError):
            next(server.do_action(None, Action("actor", b"payload")))

    def test_do_put_routes_the_batch_and_writes_back_sender_credits(self, server):
        seen = []

        def handler(command, data):
            seen.append((command, data))
            return 7

        server.register_data_handler(handler)
        data = Table.from_pydict({"x": [1, 2, 3]})
        descriptor, reader, writer = TestProxyServer.do_put_args(b"cmd", data)

        server.do_put(None, descriptor, reader, writer)

        assert seen == [(b"cmd", data)]
        # The credit is echoed back as a little-endian 8-byte integer.
        written = writer.write.call_args[0][0]
        assert written.to_pybytes() == (7).to_bytes(length=8, byteorder="little")

    def test_do_put_skips_the_credit_reply_for_a_non_int_result(self, server):
        server.register_data_handler(lambda command, data: None)
        descriptor, reader, writer = TestProxyServer.do_put_args()

        server.do_put(None, descriptor, reader, writer)

        writer.write.assert_not_called()

    def test_control_action_is_routed_to_the_registered_control_handler(self, server):
        seen = []

        def handler(control_message):
            seen.append(control_message)
            return 3

        server.register_control_handler(handler)

        result = next(server.do_action(None, Action("control", b"control-bytes")))

        assert seen == [b"control-bytes"]
        # The reply carries the queue size used for credit calculation.
        assert result.body.to_pybytes() == b"3"

    def test_actor_action_is_routed_to_the_registered_actor_handler(self, server):
        seen = []

        def handler(message):
            seen.append(message)
            return b"actor-ack"

        server.register_actor_message_handler(handler)

        result = next(server.do_action(None, Action("actor", b"actor-bytes")))

        assert seen == [b"actor-bytes"]
        assert result.body.to_pybytes() == b"actor-ack"


class TestProxyServerPort:
    def test_serves_on_the_requested_port(self):
        port = get_free_local_port()
        server = ProxyServer(port=port)
        try:
            assert server.get_port_number() == port
        finally:
            server.graceful_shutdown()

    def test_picks_a_free_port_when_none_is_given(self):
        server = ProxyServer()
        try:
            port = server.get_port_number()
            assert isinstance(port, int)
            assert 0 < port < 65536
            # A second server must not collide with the first one's port.
            other = ProxyServer()
            try:
                assert other.get_port_number() != port
            finally:
                other.graceful_shutdown()
        finally:
            server.graceful_shutdown()
