import threading
import time
from multiprocessing import Process
from queue import Queue

import pytest
from loguru import logger
from pandas import DataFrame
from pyarrow import Table, ArrowNotImplementedError
from pyarrow._flight import FlightServerError

from python_rpc_client import PythonRPCClient
from python_rpc_server import PythonRPCServer


def hello():
    return "hello-function"


class HelloClass:
    def __call__(self):
        return "hello-class"


# prepare some functions to be test for registration
test_funcs = {
    "hello": lambda: "hello",
    "this is another call": lambda: "ack!!!",
    "hello-function": hello,
    "hello-class": HelloClass(),
    "echo": lambda x: x,
    "add": lambda a, b: a + b,
    "div": lambda a, b: a / b
}


class TestFlightClient:

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
        server = PythonRPCServer()
        for name in func_names:
            server.register(name, test_funcs[name])
        server.serve()

    @staticmethod
    def start_flight_server_with_dp(*func_names):
        shared_queue = Queue()
        server = PythonRPCServer()
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

        dp_thread.join()
        network_thread.join()

    @pytest.mark.parametrize('args', [tuple()])
    def test_client_can_connect_to_server(self, launch_server):
        client = PythonRPCClient()

    @pytest.mark.parametrize('args', [tuple()])
    def test_client_can_shutdown_server(self, launch_server):
        client = PythonRPCClient()
        assert client.call("shutdown") == b'Bye bye!'

    @pytest.mark.parametrize('args', [("hello", "this is another call")])
    def test_client_can_call_registered_lambdas(self, launch_server):
        client = PythonRPCClient()
        assert len(client.list_actions()) == 3
        assert client.call("hello") == b'hello'
        assert client.call("this is another call") == b'ack!!!'
        assert client.call("shutdown") == b'Bye bye!'

    @pytest.mark.parametrize('args', [("hello-function",)])
    def test_client_can_call_registered_function(self, launch_server):
        client = PythonRPCClient()
        assert len(client.list_actions()) == 2
        assert client.call("hello-function") == b'hello-function'
        assert client.call("shutdown") == b'Bye bye!'

    @pytest.mark.parametrize('args', [("hello-class",)])
    def test_client_can_call_registered_callable_class(self, launch_server):
        client = PythonRPCClient()
        assert len(client.list_actions()) == 2
        assert client.call("hello-class") == b'hello-class'
        assert client.call("shutdown") == b'Bye bye!'

    @pytest.mark.parametrize('args', [tuple()])
    def test_client_can_send_and_receive_flights(self, launch_server):
        # prepare a dataframe and convert to pyarrow table
        df_to_sent = DataFrame({
            'Brand': ['Honda Civic', 'Toyota Corolla', 'Ford Focus', 'Audi A4'],
            'Price': [22000, 25000, 27000, 35000]
        }, columns=['Brand', 'Price'])
        table = Table.from_pandas(df_to_sent)

        client = PythonRPCClient()

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

    @pytest.mark.parametrize('args', [tuple()])
    def test_client_can_send_flights_with_dp(self, launch_server_with_dp):
        # prepare a dataframe and convert to pyarrow table
        df_to_sent = DataFrame({
            'Brand': ['Honda Civic', 'Toyota Corolla', 'Ford Focus', 'Audi A4'],
            'Price': [22000, 25000, 27000, 35000]
        }, columns=['Brand', 'Price'])
        table = Table.from_pandas(df_to_sent)

        # send the pyarrow table to server as a flight
        client = PythonRPCClient()
        client.send_data(table)

    @pytest.mark.parametrize('args', [("add",)])
    def test_client_can_call_registered_lambdas_with_args(self, launch_server):
        client = PythonRPCClient()
        assert len(client.list_actions()) == 2
        assert client.call("add", a=5, b=4) == b'9'
        assert client.call("add", a=1.1, b=2.3) == b'3.4'
        assert client.call("add", a=[1, 2, 3], b=[5]) == b'[1, 2, 3, 5]'
        assert client.call("shutdown") == b'Bye bye!'

    @pytest.mark.parametrize('args', [("div",)])
    def test_client_can_call_registered_lambdas_with_args_and_exceptions(self, launch_server):
        client = PythonRPCClient()
        assert len(client.list_actions()) == 2
        assert client.call("div", a=5, b=2) == b'2.5'
        with pytest.raises(FlightServerError):
            client.call("div", a=1, b=0)
        assert client.call("shutdown") == b'Bye bye!'
