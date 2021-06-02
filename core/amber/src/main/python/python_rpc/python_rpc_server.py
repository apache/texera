import argparse
import ast
import threading
import time
from functools import wraps

from loguru import logger
from pyarrow import py_buffer, MockOutputStream, RecordBatchStreamWriter, Table
from pyarrow.flight import FlightDescriptor
from pyarrow.flight import FlightEndpoint
from pyarrow.flight import FlightInfo
from pyarrow.flight import FlightServerBase, ServerCallContext, Action
from pyarrow.flight import Location
from pyarrow.flight import RecordBatchStream
from pyarrow.flight import Result

from common import deserialize_arguments


class PythonRPCServer(FlightServerBase):
    @staticmethod
    def ack(original_func=None, msg="ack"):
        def ack_decorator(func: callable):
            @wraps(func)
            def wrapper(*args, **kwargs):
                func(*args, **kwargs)
                return msg

            return wrapper

        if original_func:
            return ack_decorator(original_func)
        return ack_decorator

    def __init__(self, host="localhost", location="grpc+tcp://localhost:5005"):
        super(PythonRPCServer, self).__init__(location)
        logger.debug("Serving on " + location)
        self.host = host
        self.flights = {}
        self._procedures = dict()

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
    @classmethod
    def descriptor_to_key(cls, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))

    def _make_flight_info(self, key, descriptor, table: Table):
        location = Location.for_grpc_tcp(self.host, self.port)
        endpoints = [FlightEndpoint(repr(key), [location]), ]

        mock_sink = MockOutputStream()
        stream_writer = RecordBatchStreamWriter(
            mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()
        return FlightInfo(table.schema,
                          descriptor, endpoints,
                          table.num_rows, data_size)

    def list_flights(self, context, criteria):
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = \
                    FlightDescriptor.for_command(key[1])
            else:
                descriptor = FlightDescriptor.for_path(*key[2])

            yield self._make_flight_info(key, descriptor, table)

    def get_flight_info(self, context, descriptor):
        key = PythonRPCServer.descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
        raise KeyError('Flight not found.')

    def do_put(self, context, descriptor, reader, writer):
        key = PythonRPCServer.descriptor_to_key(descriptor)
        logger.debug(f"putting flight with key={key}")
        self.flights[key] = reader.read_all()
        self.process_data(self.flights[key])

    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None
        return RecordBatchStream(self.flights[key])

    ###############################
    # RPC actions related methods #
    ###############################
    def list_actions(self, context):
        return map(lambda x: (x[0], x[1][1]), self._procedures.items())

    def do_action(self, context: ServerCallContext, action: Action):
        """
        perform an action that previously registered with a procedure,
        return a result in bytes.
        :param context:
        :param action:
        :return:
        """
        procedure, _ = self._procedures.get(action.type)
        if not procedure:
            raise KeyError("Unknown action {!r}".format(action.type))

        arguments = deserialize_arguments(action.body.to_pybytes())
        logger.debug(f"calling {action.type} with args {arguments} along with context {context}")

        result = procedure(*arguments["args"], **arguments["kwargs"])
        if isinstance(result, bytes):
            encoded = result
        else:
            encoded = str(result).encode('utf-8')

        yield Result(py_buffer(encoded))

    def _shutdown(self):
        """Shut down after a delay."""
        logger.debug("Server is shutting down...")
        time.sleep(1)
        self.shutdown()

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

    def register_data_handler(self, handler: callable):
        self.process_data = handler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, default="localhost",
                        help="Address or hostname to listen on")
    parser.add_argument("--port", type=int, default=5005,
                        help="Port number to listen on")
    parser.add_argument("--tls", nargs=2, default=None,
                        metavar=('CERTFILE', 'KEYFILE'),
                        help="Enable transport-level security")
    parser.add_argument("--verify_client", type=bool, default=False,
                        help="enable mutual TLS and verify the client if True")

    args = parser.parse_args()
    tls_certificates = []
    scheme = "grpc+tcp"
    if args.tls:
        scheme = "grpc+tls"
        with open(args.tls[0], "rb") as cert_file:
            tls_cert_chain = cert_file.read()
        with open(args.tls[1], "rb") as key_file:
            tls_private_key = key_file.read()
        tls_certificates.append((tls_cert_chain, tls_private_key))

    location = "{}://{}:{}".format(scheme, args.host, args.port)

    server = PythonRPCServer(args.host, location,
                             tls_certificates=tls_certificates,
                             verify_client=args.verify_client)
    print("Serving on", location)
    server.serve()


if __name__ == '__main__':
    main()
