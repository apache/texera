import pytest
from pyarrow.flight import Action

from common import serialize_arguments
from python_rpc_server import PythonRPCServer


class TestFlightServer:
    port = 50005

    @pytest.fixture()
    def server(self):
        server = PythonRPCServer(port=TestFlightServer.port)
        TestFlightServer.port += 1
        return server

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
            def __call__(self, ):
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
            "get a dict": {"entry": [5, (None, 123.4)]}
        }

        for name, result in procedure_contents.items():
            server.register(name, lambda: result)
            assert name in server._procedures
            assert next(server.do_action(None, Action(name, b''))).body.to_pybytes() \
                   == str(result).encode('utf-8')

    def test_server_can_invoke_registered_control_actions_with_args(self, server):
        name = "echo"
        result = b"hello"
        serialized_args = serialize_arguments("hello")
        server.register(name, lambda x: x)
        assert name in server._procedures
        assert next(server.do_action(None, Action(name, serialized_args))).body.to_pybytes() \
               == result

    def test_server_can_invoke_registered_control_actions_with_args2(self, server):
        name = "add"
        result = b"3"
        serialized_args = serialize_arguments(1, 2)
        server.register(name, lambda a, b: a + b)
        assert name in server._procedures
        assert next(server.do_action(None, Action(name, serialized_args))).body.to_pybytes() \
               == result

    def test_server_can_invoke_registered_control_actions_with_args_exception(self, server):
        name = "div"
        serialized_args = serialize_arguments(1, 0)
        server.register(name, lambda a, b: a / b)
        assert name in server._procedures
        with pytest.raises(ZeroDivisionError):
            next(server.do_action(None, Action(name, serialized_args))).body.to_pybytes()

    def test_server_can_invoke_registered_lambda_with_args_and_ack(self, server):
        name = "i need an ack"
        serialized_args = serialize_arguments("some input for lambda")

        server.register(name, PythonRPCServer.ack()(lambda _: "random output"))
        assert name in server._procedures
        assert next(server.do_action(None, Action(name, serialized_args))).body.to_pybytes() \
               == b'ack'

    def test_server_can_invoke_registered_function_with_args_and_ack(self, server):
        name = "i need an ack"
        serialized_args = serialize_arguments("some input for function")

        @PythonRPCServer.ack
        def handler(_):
            return "random output"

        server.register(name, handler)
        assert name in server._procedures
        assert next(server.do_action(None, Action(name, serialized_args))).body.to_pybytes() \
               == b'ack'

    def test_server_can_invoke_registered_callable_class_with_args_and_ack(self, server):
        name = "i need an ack"
        serialized_args = serialize_arguments("some input for callable class")

        class Handler:
            @PythonRPCServer.ack
            def __call__(self, _):
                return "random output"

        server.register(name, Handler())
        assert name in server._procedures
        assert next(server.do_action(None, Action(name, serialized_args))).body.to_pybytes() \
               == b'ack'
