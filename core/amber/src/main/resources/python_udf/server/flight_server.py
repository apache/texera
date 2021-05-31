# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""An example Flight Python server."""

import argparse
import ast
import threading
import time

from loguru import logger
from pyarrow import py_buffer, MockOutputStream, RecordBatchStreamWriter
from pyarrow._flight import Action
from pyarrow.flight import FlightDescriptor
from pyarrow.flight import FlightEndpoint
from pyarrow.flight import FlightInfo
from pyarrow.flight import FlightServerBase
from pyarrow.flight import Location
from pyarrow.flight import RecordBatchStream
from pyarrow.flight import Result
from pyarrow.flight import ServerCallContext


class FlightServer(FlightServerBase):
    def __init__(self, host="localhost", location=None,
                 tls_certificates=None, verify_client=False,
                 root_certificates=None, auth_handler=None):
        super(FlightServer, self).__init__(
            location, auth_handler, tls_certificates, verify_client,
            root_certificates)
        logger.info("Serving on " + location)
        self.host = host
        self.flights = {}
        self.callbacks = dict()

        self.register("shutdown", lambda: threading.Thread(target=self._shutdown).start(), description="Shut down this server.")

    ###########################
    # Flights related methods #
    ###########################
    @classmethod
    def descriptor_to_key(self, descriptor):
        return (descriptor.descriptor_type.value, descriptor.command,
                tuple(descriptor.path or tuple()))

    def _make_flight_info(self, key, descriptor, table):
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
        key = FlightServer.descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
        raise KeyError('Flight not found.')

    def do_put(self, context, descriptor, reader, writer):
        key = FlightServer.descriptor_to_key(descriptor)
        print(key)
        self.flights[key] = reader.read_all()
        print(self.flights[key])

    def do_get(self, context, ticket):
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            return None
        return RecordBatchStream(self.flights[key])

    ###############################
    # RPC actions related methods #
    ###############################
    def list_actions(self, context):
        return map(lambda x: (x[0], x[1][1]), self.callbacks.items())

    def do_action(self, context: ServerCallContext, action: Action):
        callback, _ = self.callbacks.get(action.type)
        if not callback:
            raise KeyError("Unknown action {!r}".format(action.type))
        else:
            result = callback()
            if isinstance(result, str):
                encoded = result.encode('utf-8')
            elif isinstance(result, bytes):
                encoded = result
            else:
                encoded = b''

            yield Result(py_buffer(encoded))

    def _shutdown(self):
        """Shut down after a delay."""
        logger.info("Server is shutting down...")
        time.sleep(1)
        self.shutdown()

    def register(self, name: str, procedure: callable, description: str = "") -> None:

        @logger.catch(reraise=True)
        def wrapper(*args, **kwargs):
            return procedure(*args, **kwargs)

        self.callbacks[name] = (wrapper, description)
        logger.info("registered procedure " + name)


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

    server = FlightServer(args.host, location,
                          tls_certificates=tls_certificates,
                          verify_client=args.verify_client)
    print("Serving on", location)
    server.serve()


if __name__ == '__main__':
    main()
