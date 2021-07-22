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
        yield ProxyServer()

    @pytest.fixture
    def server_with_dp(self, data_queue):
        server = ProxyServer()
        server.register_data_handler(lambda cmd, table:
                                     list(map(data_queue.put,
                                              map(lambda t: t[1], table.to_pandas().iterrows()))))
        yield server

    @pytest.fixture
    def client(self):
        yield ProxyClient()

    @pytest.fixture
    def data_table(self):
        df_to_sent = DataFrame({
            'Brand': ['Honda Civic', 'Toyota Corolla', 'Ford Focus', 'Audi A4'],
            'Price': [22000, 25000, 27000, 35000]
        }, columns=['Brand', 'Price'])
        return Table.from_pandas(df_to_sent)

    def test_client_can_connect_to_server(self, server):
        with server:
            ProxyClient()

    def test_client_can_shutdown_server(self, server, client):
        with server:
            assert client.call_action("shutdown") == b'Bye bye!'

    def test_client_can_call_registered_lambdas(self, server, client):
        with server:
            action_count = len(client.list_actions())
            server.register("hello", lambda: "hello")
            server.register("this is another call", lambda: "ack!!!")
            assert len(client.list_actions()) == action_count + 2
            assert client.call_action("hello") == b'hello'
            assert client.call_action("this is another call") == b'ack!!!'
            assert client.call_action("shutdown") == b'Bye bye!'

    def test_client_can_call_registered_function(self, server, client):
        with server:
            def hello():
                return "hello-function"
            action_count = len(client.list_actions())
            server.register("hello-function", hello)
            assert len(client.list_actions()) == action_count + 1
            assert client.call_action("hello-function") == b'hello-function'
            assert client.call_action("shutdown") == b'Bye bye!'

    def test_client_can_call_registered_callable_class(self, server, client):
        with server:
            class HelloClass:
                def __call__(self):
                    return "hello-class"

            action_count = len(client.list_actions())
            server.register("hello-class", HelloClass())
            assert len(client.list_actions()) == action_count + 1
            assert client.call_action("hello-class") == b'hello-class'
            assert client.call_action("shutdown") == b'Bye bye!'

    # def test_client_can_send_control_message(self, server, client):
    #     with server:
    #         server.register_control_handler()

    # def test_client_can_call_registered_lambdas_with_args(self, server, client):
    #     with server:
    #         server.register("echo", lambda x: x)
    #         assert len(client.list_actions()) == 2
    #         assert client.call_action("echo", "no") == b'no'
    #         assert client.call_action("shutdown") == b'Bye bye!'
    #
    # def test_client_can_call_registered_lambdas_with_args2(self, server, client):
    #     with server:
    #         server.register("add", lambda a, b: a + b)
    #         assert len(client.list_actions()) == 2
    #         assert client.call_action("add", a=5, b=4) == b'9'
    #         assert client.call_action("add", 1.1, 2.3) == b'3.4'
    #         assert client.call_action("add", a=[1, 2, 3], b=[5]) == b'[1, 2, 3, 5]'
    #         assert client.call_action("shutdown") == b'Bye bye!'
    #
    # def test_client_can_call_registered_lambdas_with_args_and_ack(self, server, client):
    #     with server:
    #         server.register("add", ProxyServer.ack()(lambda a, b: a + b))
    #         assert len(client.list_actions()) == 2
    #         assert client.call_action("add", a=5, b=4) == b'ack'
    #         assert client.call_action("add", 1.1, 2.3) == b'ack'
    #         assert client.call_action("add", a=[1, 2, 3], b=[5]) == b'ack'
    #         assert client.call_action("shutdown") == b'Bye bye!'
    #
    # def test_client_can_call_registered_lambdas_with_args_and_other_ack(self, server, client):
    #     class Extend:
    #         @ProxyServer.ack(msg="extended!")
    #         def __call__(self, a: list, b: Iterator):
    #             a.extend(b)
    #             return a
    #
    #     def extend(a: list, b: Iterator):
    #         a.extend(b)
    #         return a
    #
    #     with server:
    #         server.register("extend_with_ack", Extend())
    #         server.register("extend_with_result", extend)
    #         assert len(client.list_actions()) == 3
    #         assert client.call_action("extend_with_ack", [5], (4,)) == b'extended!'
    #         assert client.call_action("extend_with_result", [], "hello") == b"['h', 'e', 'l', 'l', 'o']"
    #         assert client.call_action("extend_with_result", a=[1, 2, 3], b=[5]) == b'[1, 2, 3, 5]'
    #         assert client.call_action("shutdown") == b'Bye bye!'
    #
    # def test_client_can_call_registered_lambdas_with_args_and_exceptions(self, server, client):
    #     with server:
    #         server.register("div", lambda a, b: a / b)
    #         assert len(client.list_actions()) == 2
    #         assert client.call_action("div", a=5, b=2) == b'2.5'
    #         with pytest.raises(FlightServerError):
    #             client.call_action("div", a=1, b=0)
    #         assert client.call_action("shutdown") == b'Bye bye!'

    def test_client_cannot_send_data_without_handler(self, server, client, data_table):
        with server:
            # send the pyarrow table to server as a flight
            with pytest.raises(ArrowNotImplementedError):
                client.send_data(command=bytes(), table=data_table)

    def test_client_can_send_data_with_handler(self, data_queue: Queue, server_with_dp, client, data_table):
        with server_with_dp:
            # send the pyarrow table to server as a flight
            client.send_data(bytes(), data_table)

            assert data_queue.qsize() == 4
            for i, row in data_table.to_pandas().iterrows():
                assert data_queue.get().equals(row)
