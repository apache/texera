import threading
import time
from multiprocessing import Process
from queue import Queue

import pytest
from loguru import logger
from pandas import DataFrame
from pyarrow import Table

from server.flight_server import FlightServer
from server.mock_client import UDFMockClient


def hello(_):
    return "hello-function"


class HelloClass:
    def __call__(self, _):
        return "hello-class"


# prepare some functions to be test for registration
test_funcs = {
    "hello": lambda _: "hello",
    "this is another call": lambda _: "ack!!!",
    "hello-function": hello,
    "hello-class": HelloClass(),
}


class TestSeverIntegration:

    @pytest.fixture
    def launch_server(self, args):
        p1 = Process(target=self.start_flight_server, args=args)
        p1.start()
        time.sleep(1)
        yield
        p1.kill()

    @pytest.fixture
    def launch_server_with_dp(self, args):
        p1 = Process(target=self.start_flight_server_with_dp, args=args)
        p1.start()
        time.sleep(1)
        yield
        p1.kill()

    @staticmethod
    def start_flight_server(*func_names):
        host: str = "localhost"
        port: int = 5005
        scheme: str = "grpc+tcp"
        location = f"{scheme}://{host}:{port}"
        server = FlightServer(host, location)
        for name in func_names:
            server.register(name, test_funcs[name])
        server.serve()

    @staticmethod
    def start_flight_server_with_dp(*func_names):
        shared_queue = Queue()

        host: str = "localhost"
        location = f"grpc+tcp://{host}:5005"
        server = FlightServer(host, location)
        for name in func_names:
            server.register(name, test_funcs[name])

        def _start_server(q):
            server.register_data_handler(lambda batch: list(map(q.put, batch.to_pandas().iterrows())))

        network_thread = threading.Thread(target=_start_server, args=(shared_queue,))
        network_thread.start()

        def _start_dp(q):
            while True:
                logger.info(q.get())

        dp_thread = threading.Thread(target=_start_dp, args=(shared_queue,))
        dp_thread.start()

    @pytest.mark.parametrize('args', [tuple()])
    def test_server_can_start(self, launch_server):
        client = UDFMockClient()
        # should by default only have shutdown
        assert len(client.list_actions()) == 1
        action = client.list_actions()[0]
        assert action.type == "shutdown"
        assert action.description == "Shut down this server."

    @pytest.mark.parametrize('args', [tuple()])
    def test_server_can_shutdown_by_client(self, launch_server):
        client = UDFMockClient()
        assert client.call("shutdown") == b''

    @pytest.mark.parametrize('args', [("hello", "this is another call")])
    def test_server_call_registered_lambdas(self, launch_server):
        client = UDFMockClient()
        assert len(client.list_actions()) == 3
        assert client.call("hello") == b'hello'
        assert client.call("this is another call") == b'ack!!!'
        assert client.call("shutdown") == b''

    @pytest.mark.parametrize('args', [("hello-function",)])
    def test_server_call_registered_function(self, launch_server):
        client = UDFMockClient()
        assert len(client.list_actions()) == 2
        assert client.call("hello-function") == b'hello-function'
        assert client.call("shutdown") == b''

    @pytest.mark.parametrize('args', [("hello-class",)])
    def test_server_call_registered_callable_class(self, launch_server):
        client = UDFMockClient()
        assert len(client.list_actions()) == 2
        assert client.call("hello-class") == b'hello-class'
        assert client.call("shutdown") == b''

    @pytest.mark.parametrize('args', [tuple()])
    def test_server_receive_flights(self, launch_server_with_dp):
        # prepare a dataframe and convert to pyarrow table
        df_to_sent = DataFrame({
            'Brand': ['Honda Civic', 'Toyota Corolla', 'Ford Focus', 'Audi A4'],
            'Price': [22000, 25000, 27000, 35000]
        }, columns=['Brand', 'Price'])
        table = Table.from_pandas(df_to_sent)

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
        client = UDFMockClient()
        client.send_data(table, on_success=_handler)

    @pytest.mark.parametrize('args', [tuple()])
    def test_server_receive_flights_for_dp(self, launch_server_with_dp):
        # prepare a dataframe and convert to pyarrow table
        df_to_sent = DataFrame({
            'Brand': ['Honda Civic', 'Toyota Corolla', 'Ford Focus', 'Audi A4'],
            'Price': [22000, 25000, 27000, 35000]
        }, columns=['Brand', 'Price'])
        table = Table.from_pandas(df_to_sent)

        # send the pyarrow table to server as a flight
        client = UDFMockClient()
        client.send_data(table)
