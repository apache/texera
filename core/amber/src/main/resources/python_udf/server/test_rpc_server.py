import time
from multiprocessing import Process

import pytest

from server.flight_server import FlightServer
from server.mock_client import UDFMockClient


def add(a: int, b: int) -> int:
    return a + b


@pytest.fixture()
def launch_server(args):
    p1 = Process(target=server, args=args)
    p1.start()
    time.sleep(1)
    yield
    p1.kill()


def server(*args):
    print(args)
    host: str = "localhost"
    port: int = 5005
    scheme: str = "grpc+tcp"
    location = "{}://{}:{}".format(scheme, host, port)

    server = FlightServer(host, location)

    server.register("hello", lambda: "hello")
    server.register("this is another call", lambda: "ack!!!")
    server.serve()


@pytest.mark.parametrize('args', [tuple()])
def test_server_can_start(launch_server):
    client = UDFMockClient()


@pytest.mark.parametrize('args', [('hello', 'this')])
def test_basic_server_client(launch_server):
    client = UDFMockClient()
    assert len(client.list_actions()) == 3
    print(client.call("hello"))
    print(client.call("this is another call"))
    print(client.call("shutdown"))
    print("done")
