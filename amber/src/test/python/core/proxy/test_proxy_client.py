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

import pytest
from pandas import DataFrame
from pyarrow import ArrowNotImplementedError, Table
from queue import Queue

import core.proxy.proxy_client as proxy_client_module
from core.proxy.proxy_client import ProxyClient
from core.proxy.proxy_server import ProxyServer


class TestProxyClient:
    @pytest.fixture
    def data_queue(self):
        return Queue()

    @pytest.fixture
    def server(self):
        server = ProxyServer(port=5005)
        yield server
        server.graceful_shutdown()

    @pytest.fixture
    def server_with_dp(self, data_queue):
        server = ProxyServer(port=5005)
        server.register_data_handler(
            lambda _, table: list(
                map(data_queue.put, map(lambda t: t[1], table.to_pandas().iterrows()))
            )
        )
        yield server
        server.graceful_shutdown()

    class MockFlightMetadataReader:
        """
        MockFlightMetadataReader is a mocked FlightMetadataReader class to ultimately
        mock a credit value to be returned from Scala server to Python client
        """

        class MockBuffer:
            def to_pybytes(self):
                dummy_credit = 31
                return dummy_credit.to_bytes(8, "little")

        def read(self):
            return self.MockBuffer()

    @pytest.fixture
    def client(self):
        mock_client = ProxyClient()

        def mock_do_put(
            self,
            FlightDescriptor_descriptor,
            Schema_schema,
            FlightCallOptions_options=None,
        ):
            """
            Mocking FlightClient.do_put that is called in ProxyClient to return
            a MockFlightMetadataReader instead of a FlightMetadataReader

            :param self: an instance of FlightClient (would be ProxyClient in this case)
            :param FlightDescriptor_descriptor: descriptor
            :param Schema_schema: schema
            :param FlightCallOptions_options: options, None by default
            :return: writer : FlightStreamWriter, reader : MockFlightMetadataReader
            """
            writer, _ = super(ProxyClient, self).do_put(
                FlightDescriptor_descriptor, Schema_schema, FlightCallOptions_options
            )
            reader = TestProxyClient.MockFlightMetadataReader()
            return writer, reader

        mock_client.do_put = mock_do_put.__get__(
            mock_client, ProxyClient
        )  # override do_put with mock_do_put

        yield mock_client

    @pytest.fixture
    def data_table(self):
        df_to_sent = DataFrame(
            {
                "Brand": ["Honda Civic", "Toyota Corolla", "Ford Focus", "Audi A4"],
                "Price": [22000, 25000, 27000, 35000],
            },
            columns=["Brand", "Price"],
        )
        return Table.from_pandas(df_to_sent)

    def test_client_can_connect_to_server(self, server, client):
        assert client.call_action("heartbeat") == b"ack"

    def test_client_can_shutdown_server(self, server, client):
        assert client.call_action("shutdown") == b"Bye bye!"

    def test_client_can_call_registered_lambdas(self, server, client):
        action_count = len(client.list_actions())
        server.register("hello", lambda: "hello")
        server.register("this is another call", lambda: "ack!!!")
        assert len(client.list_actions()) == action_count + 2
        assert client.call_action("hello") == b"hello"
        assert client.call_action("this is another call") == b"ack!!!"
        assert client.call_action("shutdown") == b"Bye bye!"

    def test_client_can_call_registered_function(self, server, client):
        def hello():
            return "hello-function"

        action_count = len(client.list_actions())
        server.register("hello-function", hello)
        assert len(client.list_actions()) == action_count + 1
        assert client.call_action("hello-function") == b"hello-function"
        assert client.call_action("shutdown") == b"Bye bye!"

    def test_client_can_call_registered_callable_class(self, server, client):
        class HelloClass:
            def __call__(self):
                return "hello-class"

        action_count = len(client.list_actions())
        server.register("hello-class", HelloClass())
        assert len(client.list_actions()) == action_count + 1
        assert client.call_action("hello-class") == b"hello-class"
        assert client.call_action("shutdown") == b"Bye bye!"

    def test_client_cannot_send_data_without_handler(self, server, client, data_table):
        # send the pyarrow table to server as a flight
        with pytest.raises(ArrowNotImplementedError):
            client.send_data(command=bytes(), table=data_table)

    def test_client_can_send_data_with_handler(
        self, data_queue: Queue, server_with_dp, client, data_table
    ):
        # send the pyarrow table to server as a flight
        client.send_data(bytes(), data_table)

        assert data_queue.qsize() == 4
        for i, row in data_table.to_pandas().iterrows():
            assert data_queue.get().equals(row)

    def test_a_handshake_port_is_announced_to_the_server_on_connect(self, server):
        # The Java side learns which port the Python proxy *server* listens on
        # through a "handshake" action issued while the client connects.
        # ProxyServer does not register "handshake" itself, so the test
        # registers a capturing stand-in for the Java handler.
        received = []
        server.register("handshake", lambda payload: received.append(payload) or "ok")

        client = ProxyClient(handshake_port=6789)
        try:
            # The payload must be the handshake port rendered as UTF-8 digits,
            # not the client's timeout or the data port it dialled. (The action
            # *name* is pinned separately, by the test below -- see why there.)
            assert received == [b"6789"]
        finally:
            client.close()

    def test_the_handshake_call_names_the_handshake_action(self, monkeypatch):
        # The end-to-end test above cannot pin the action *name*. Substituting
        # any other registered name makes construction blow up server-side (the
        # built-in `heartbeat`/`shutdown` handlers take no argument, while
        # `control`/`actor` try to deserialize the payload), so the test dies
        # during `ProxyClient(...)` and its `received` assertion never runs --
        # the name is pinned by an accident of the fixture, not by an
        # assertion. Stub the call at the client boundary instead, so the name
        # reaches an assertion rather than a server-side crash.
        seen = []
        monkeypatch.setattr(
            ProxyClient,
            "call_action",
            lambda self, name, payload=bytes(), options=None: seen.append(
                (name, payload)
            ),
        )

        client = ProxyClient(handshake_port=6789)
        try:
            assert seen == [("handshake", b"6789")]
        finally:
            client.close()

    def test_no_handshake_is_sent_when_no_handshake_port_is_given(self, server):
        # Companion negative case: without it, unconditionally calling
        # `_handshake` would still satisfy the test above.
        received = []
        server.register("handshake", lambda payload: received.append(payload) or "ok")

        client = ProxyClient()
        try:
            assert received == []
            # ... and the client is still usable, i.e. skipping the handshake
            # is a real branch rather than a failed connect.
            assert client.call_action("heartbeat") == b"ack"
        finally:
            client.close()

    @staticmethod
    def _record_call_options(monkeypatch):
        """
        Record every `FlightCallOptions(...)` `call_action` manufactures.

        A plain factory function rather than a subclass: `FlightCallOptions` is
        a Cython cdef class. It still delegates to the real constructor, so the
        RPC underneath stays a real one.
        """
        real = proxy_client_module.FlightCallOptions
        captured = []

        def recording_factory(**kwargs):
            captured.append(kwargs)
            return real(**kwargs)

        monkeypatch.setattr(proxy_client_module, "FlightCallOptions", recording_factory)
        return captured, real

    def test_the_configured_timeout_is_applied_when_no_options_are_supplied(
        self, server, monkeypatch
    ):
        # This is the only place the client's configured timeout ever reaches
        # an RPC, and nothing constrained it: dropping the argument entirely
        # (`FlightCallOptions()`) survived the whole suite. Without a timeout a
        # hung Java-side server would block the Python worker forever instead
        # of raising.
        captured, _ = self._record_call_options(monkeypatch)

        # A non-default timeout, so the assertion pins the value *flowing from
        # the constructor* rather than a hard-coded 1000.
        client = ProxyClient(timeout=1234)
        try:
            assert client.call_action("heartbeat") == b"ack"
        finally:
            client.close()

        assert captured == [{"timeout": 1234}]

    def test_caller_supplied_options_are_used_as_is(self, server, monkeypatch):
        # The companion arm: when the caller hands in an options object,
        # `call_action` must not manufacture its own and discard it.
        captured, real = self._record_call_options(monkeypatch)

        client = ProxyClient(timeout=1234)
        try:
            options = real(timeout=99)
            assert client.call_action("heartbeat", options=options) == b"ack"
        finally:
            client.close()

        assert captured == []
