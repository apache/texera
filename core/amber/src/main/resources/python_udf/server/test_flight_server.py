import time
from multiprocessing import Process

import pytest

from server.flight_server import FlightServer
from server.mock_client import UDFMockClient


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
    "echo-data": lambda x: x.get
}


class TestSeverIntegration:

    @pytest.fixture
    def launch_server(self, args):
        p1 = Process(target=self.start_flight_server, args=args)
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

    @pytest.mark.parametrize('args', [tuple()])
    def test_server_can_start(self, launch_server):
        client = UDFMockClient()
        # should by default only have shutdown and process_data
        assert len(client.list_actions()) == 2
        action = sorted(client.list_actions(), key=lambda x: x.type)[1]
        assert action.type == "shutdown"
        assert action.description == "Shut down this server."

    @pytest.mark.parametrize('args', [tuple()])
    def test_server_can_shutdown_by_client(self, launch_server):
        client = UDFMockClient()
        assert client.call("shutdown") == b''

    @pytest.mark.parametrize('args', [("hello", "this is another call")])
    def test_server_call_registered_lambdas(self, launch_server):
        client = UDFMockClient()
        assert len(client.list_actions()) == 4
        assert client.call("hello") == b'hello'
        assert client.call("this is another call") == b'ack!!!'
        assert client.call("shutdown") == b''

    @pytest.mark.parametrize('args', [("hello-function",)])
    def test_server_call_registered_function(self, launch_server):
        client = UDFMockClient()
        assert len(client.list_actions()) == 3
        assert client.call("hello-function") == b'hello-function'
        assert client.call("shutdown") == b''

    @pytest.mark.parametrize('args', [("hello-class",)])
    def test_server_call_registered_callable_class(self, launch_server):
        client = UDFMockClient()
        assert len(client.list_actions()) == 3
        assert client.call("hello-class") == b'hello-class'
        assert client.call("shutdown") == b''

    @pytest.mark.parametrize('args', [tuple()])
    def test_server_receive_flights(self, launch_server):
        client = UDFMockClient()
        client.push_data()
        client.push_data()
        flight_infos = list(client.list_flights())
        # assert len(flight_infos) == 2

        info = flight_infos[0]

        # assert info.total_records == 4
        #
        # assert info.total_bytes == 1096

        for endpoint in info.endpoints:
            print('Ticket:', endpoint.ticket)
            for location in endpoint.locations:
                reader = client.do_get(endpoint.ticket)
                df = reader.read_pandas()
                print(df)
