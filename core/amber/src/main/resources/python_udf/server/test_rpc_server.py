import time
from multiprocessing import Process

import pytest

from server.flight_server import FlightServer
from server.mock_client import UDFMockClient

test_funcs = {
    "hello": lambda: "hello",
    "this is another call": lambda: "ack!!!"
}


@pytest.fixture()
def launch_server(args):
    p1 = Process(target=server, args=args)
    p1.start()
    time.sleep(1)
    yield
    p1.kill()


def server(*funcs):
    print(funcs)
    host: str = "localhost"
    port: int = 5005
    scheme: str = "grpc+tcp"
    location = "{}://{}:{}".format(scheme, host, port)

    server = FlightServer(host, location)
    for name in funcs:
        server.register(name, test_funcs[name])
    server.serve()


@pytest.mark.parametrize('args', [tuple()])
def test_server_can_start(launch_server):
    client = UDFMockClient()
    # should by default only have shutdown
    assert len(client.list_actions()) == 1
    action = client.list_actions()[0]
    assert action.type == "shutdown"
    assert action.description == "Shut down this server."


@pytest.mark.parametrize('args', [("hello", "this is another call")])
def test_basic_server_client(launch_server):
    client = UDFMockClient()
    assert len(client.list_actions()) == 3
    assert client.call("hello") == b'hello'
    assert client.call("this is another call") == b'ack!!!'
    assert client.call("shutdown") == b''
