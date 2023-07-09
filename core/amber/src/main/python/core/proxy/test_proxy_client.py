from queue import Queue

import pytest
from pandas import DataFrame
from pyarrow import ArrowNotImplementedError, Table

from .proxy_client import ProxyClient
from .proxy_server import ProxyServer


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

    @pytest.fixture
    def client(self):
        yield ProxyClient()

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
