from queue import Queue

import pytest
from pandas import DataFrame
from pyarrow import Table, ArrowNotImplementedError
from pyarrow.flight import FlightServerError

from python_rpc_client import PythonRPCClient
from python_rpc_server import PythonRPCServer


class TestFlightClient:

    @pytest.fixture
    def data_queue(self):
        return Queue()

    @pytest.fixture
    def server(self):
        yield PythonRPCServer()

    @pytest.fixture
    def server_with_dp(self, data_queue):
        server = PythonRPCServer()
        server.register_data_handler(lambda batch: list(map(data_queue.put, batch.to_pandas().iterrows())))
        yield server

    @pytest.fixture
    def client(self):
        yield PythonRPCClient()

    def test_client_can_connect_to_server(self, server):
        with server:
            PythonRPCClient()

    def test_client_can_shutdown_server(self, server, client):
        with server:
            assert client.call("shutdown") == b'Bye bye!'

    def test_client_can_call_registered_lambdas(self, server, client):
        with server:
            server.register("hello", lambda: "hello")
            server.register("this is another call", lambda: "ack!!!")
            assert len(client.list_actions()) == 3
            assert client.call("hello") == b'hello'
            assert client.call("this is another call") == b'ack!!!'
            assert client.call("shutdown") == b'Bye bye!'

    def test_client_can_call_registered_function(self, server, client):
        with server:
            def hello():
                return "hello-function"

            server.register("hello-function", hello)
            assert len(client.list_actions()) == 2
            assert client.call("hello-function") == b'hello-function'
            assert client.call("shutdown") == b'Bye bye!'

    def test_client_can_call_registered_callable_class(self, server):
        with server:
            class HelloClass:
                def __call__(self):
                    return "hello-class"

            server.register("hello-class", HelloClass())
            client = PythonRPCClient()
            assert len(client.list_actions()) == 2
            assert client.call("hello-class") == b'hello-class'
            assert client.call("shutdown") == b'Bye bye!'

    def test_client_can_call_registered_lambdas_with_args(self, server, client):
        with server:
            server.register("echo", lambda x: x)
            assert len(client.list_actions()) == 2
            assert client.call("echo", "no") == b'no'
            assert client.call("shutdown") == b'Bye bye!'

    def test_client_can_call_registered_lambdas_with_args2(self, server, client):
        with server:
            server.register("add", lambda a, b: a + b)
            assert len(client.list_actions()) == 2
            assert client.call("add", a=5, b=4) == b'9'
            assert client.call("add", 1.1, 2.3) == b'3.4'
            assert client.call("add", a=[1, 2, 3], b=[5]) == b'[1, 2, 3, 5]'
            assert client.call("shutdown") == b'Bye bye!'

    def test_client_can_call_registered_lambdas_with_args_and_ack(self, server, client):
        with server:
            server.register("add", PythonRPCServer.ack()(lambda a, b: a + b))
            assert len(client.list_actions()) == 2
            assert client.call("add", a=5, b=4) == b'ack'
            assert client.call("add", 1.1, 2.3) == b'ack'
            assert client.call("add", a=[1, 2, 3], b=[5]) == b'ack'
            assert client.call("shutdown") == b'Bye bye!'

    def test_client_can_call_registered_lambdas_with_args_and_exceptions(self, server, client):
        with server:
            server.register("div", lambda a, b: a / b)
            assert len(client.list_actions()) == 2
            assert client.call("div", a=5, b=2) == b'2.5'
            with pytest.raises(FlightServerError):
                client.call("div", a=1, b=0)
            assert client.call("shutdown") == b'Bye bye!'

    def test_client_can_send_and_receive_flights(self, server, client):
        # prepare a dataframe and convert to pyarrow table
        df_to_sent = DataFrame({
            'Brand': ['Honda Civic', 'Toyota Corolla', 'Ford Focus', 'Audi A4'],
            'Price': [22000, 25000, 27000, 35000]
        }, columns=['Brand', 'Price'])
        table = Table.from_pandas(df_to_sent)

        with server:
            def _handler():
                # retrieve the same flight
                flight_infos = list(client.list_flights())
                assert len(flight_infos) == 1
                info = flight_infos[0]
                assert info.total_records == 4
                assert info.total_bytes == 1096
                for endpoint in info.endpoints:
                    reader = client.do_get(endpoint.ticket)
                    df = reader.read_pandas()
                    assert df.equals(df_to_sent)

            # send the pyarrow table to server as a flight
            with pytest.raises(ArrowNotImplementedError):
                client.send_data(table, _handler)

    def test_client_can_send_flights_with_dp(self, data_queue, server_with_dp, client):
        # prepare a dataframe and convert to pyarrow table
        df_to_sent = DataFrame({
            'Brand': ['Honda Civic', 'Toyota Corolla', 'Ford Focus', 'Audi A4'],
            'Price': [22000, 25000, 27000, 35000]
        }, columns=['Brand', 'Price'])
        table = Table.from_pandas(df_to_sent)

        with server_with_dp:
            # send the pyarrow table to server as a flight
            client.send_data(table)

            assert data_queue.qsize() == 4
