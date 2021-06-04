import argparse
import json
import threading
import time
from functools import wraps
from inspect import signature
from typing import Iterator, Tuple, Dict

from loguru import logger
from pyarrow import py_buffer, Table
from pyarrow._flight import FlightDescriptor
from pyarrow.flight import FlightServerBase, ServerCallContext, Action
from pyarrow.flight import Result
from pyarrow.ipc import RecordBatchReader, RecordBatchStreamWriter

from common import deserialize_arguments


class PythonRPCServer(FlightServerBase):
    @staticmethod
    def ack(original_func=None, msg="ack"):
        """
        decorator for returning an ack message after the action.
        example usage:
            ```
            @ack
            def hello():
                return None
            server.register("hello", hello)
            msg = client.call("hello") # msg will be "ack"
            ```

            or
            ```
            @ack(msg="other msg")
            def hello():
                return None
            server.register("hello", hello)
            msg = client.call("hello") # msg will be "other msg"
            ```

        :param original_func: decorated function, usually is a callable to be registered.
        :param msg: the return message from the decorator, "ack" by default.
        :return:
        """

        def ack_decorator(func: callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                func(*args, **kwargs)
                return msg

            return wrapper

        if original_func:
            return ack_decorator(original_func)
        return ack_decorator

    def __init__(self, scheme: str = "grpc+tcp", host: str = "localhost", port: int = 5005):
        location = f"{scheme}://{host}:{port}"
        super(PythonRPCServer, self).__init__(location)
        logger.debug("Serving on " + location)

        self.host = host

        # PyArrow Flights, identified by the ticket.
        self._flights: Dict[str, Table] = dict()

        # procedures for actions, will contain registered actions, identified by procedure name.
        self._procedures: Dict[str, Tuple[callable, str]] = dict()

        # register shutdown, this is the default action for client to terminate server.
        self.register("shutdown",
                      PythonRPCServer.ack(msg="Bye bye!")
                      (lambda: threading.Thread(target=self._shutdown).start()),
                      description="Shut down this server.")

        # the data message handlers for each batch, needs to be implemented during runtime.
        self.register_data_handler(lambda *args, **kwargs: (_ for _ in ()).throw(NotImplementedError))

    ###########################
    # Flights related methods #
    ###########################

    def do_put(self, context: ServerCallContext, descriptor: FlightDescriptor, reader: RecordBatchReader, writer: RecordBatchStreamWriter):
        """
        put a data table into server, the data will be handled by the `self.process_data()` handler.
        :param context: server context, containing information of middlewares.
        :param descriptor: the descriptor.path contains the sequence numbers for this batch of data.
        :param reader: the input stream of batches of records.
        :param writer: the output stream.
        :return:
        """
        seq_nums = json.loads(descriptor.path[0])
        logger.debug(f"putting flight with seq={seq_nums}")
        self.process_data(reader.read_all())

    ###############################
    # RPC actions related methods #
    ###############################
    def list_actions(self, context: ServerCallContext) -> Iterator[Tuple[str, str]]:
        """
        list all actions that are being registered with the server, it will
        return the procedure name and description for each registered action.
        :param context: server context, containing information of middlewares.
        :return: iterator of (procedure_name, procedure_description) pairs.
        """
        return map(lambda x: (x[0], x[1][1]), self._procedures.items())

    def do_action(self, context: ServerCallContext, action: Action) -> Iterator[Result]:
        """
        perform an action that previously registered with a procedure,
        return a result in bytes.
        :param context: server context, containing information of middlewares.
        :param action: the action to perform, including
                        action.type: the procedure name to invoke
                        action.body: the procedure arguments in bytes
        :return: yield the encoded result back to client.
        """

        # get procedure by name
        procedure, _ = self._procedures.get(action.type)
        if not procedure:
            raise KeyError("Unknown action {!r}".format(action.type))

        # parse arguments for the procedure
        arguments = deserialize_arguments(action.body.to_pybytes())
        logger.debug(f"calling {action.type} with args {arguments} along with context {context}")

        # invoke the procedure
        result = procedure(*arguments["args"], **arguments["kwargs"])

        # serialize the result
        if isinstance(result, bytes):
            encoded = result
        else:
            encoded = str(result).encode('utf-8')

        yield Result(py_buffer(encoded))

    def register(self, name: str, procedure: callable, description: str = "") -> None:
        """
        register a procedure with an action name.
        :param name: the name of the procedure, it should be matching Action's type.
        :param procedure: a callable, could be class, function, or lambda.
        :param description: describes the procedure.
        :return:
        """

        # wrap the given procedure so that its error can be logged.
        @logger.catch(level="WARNING", reraise=True)
        def wrapper(*args, **kwargs):
            return procedure(*args, **kwargs)

        # update the procedures, which overwrites the previous registration.
        self._procedures[name] = (wrapper, description)
        logger.debug("registered procedure " + name)

    def register_data_handler(self, handler: callable) -> None:
        """
        register the data handler function, which will be invoked after each `do_put`.
        :param handler: a callable with at least one argument, for the data batch.
        :return:
        """

        # the handler at least should have an argument for the data batch.
        assert len(signature(handler).parameters) >= 1

        self.process_data = handler

    ##################
    # helper methods #
    ##################
    def _shutdown(self):
        """Shut down after a delay."""
        logger.debug("Server is shutting down...")
        time.sleep(1)
        self.shutdown()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost",
                        help="Address or hostname to listen on")
    parser.add_argument("--port", type=int, default=5005,
                        help="Port number to listen on")

    args = parser.parse_args()
    scheme = "grpc+tcp"

    server = PythonRPCServer(scheme, args.host, args.port)
    server.serve()


if __name__ == '__main__':
    main()
