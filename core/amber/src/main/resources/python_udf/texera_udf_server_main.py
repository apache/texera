import ast
import importlib.util
import json
import sys
import threading
import traceback
from typing import Dict

import pandas
import pyarrow
import pyarrow.flight
from pyarrow._flight import FlightDescriptor

import texera_udf_operator_base


class UDFServer(pyarrow.flight.FlightServerBase):
    def __init__(self, udf_op, host: str = "localhost", location=None, tls_certificates=None, auth_handler=None):
        super(UDFServer, self).__init__(location, auth_handler, tls_certificates)
        self.flights: Dict = {}
        self.host: str = host
        self.tls_certificates = tls_certificates
        self.udf_op = udf_op

    @classmethod
    def _descriptor_to_key(cls, descriptor: FlightDescriptor):
        return descriptor.descriptor_type.value, descriptor.command, tuple(descriptor.path or tuple())

    def _make_flight_info(self, key, descriptor: FlightDescriptor, table):
        """NOT USED NOW"""
        if self.tls_certificates:
            location = pyarrow.flight.Location.for_grpc_tls(self.host, self.port)
        else:
            location = pyarrow.flight.Location.for_grpc_tcp(self.host, self.port)
        endpoints = [pyarrow.flight.FlightEndpoint(repr(key), [location]), ]

        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()

        return pyarrow.flight.FlightInfo(table.schema,
                                         descriptor, endpoints,
                                         table.num_rows, data_size)

    def list_flights(self, context, criteria):
        """

        NOT USED NOW

        Getting a list of available datasets on the server. This method is not used here,
        but might be useful in the future.
        """
        for key, table in self.flights.items():
            if key[1] is not None:
                descriptor = pyarrow.flight.FlightDescriptor.for_command(key[1])
            else:
                descriptor = pyarrow.flight.FlightDescriptor.for_path(*key[2])

            yield self._make_flight_info(key, descriptor, table)

    def get_flight_info(self, context, descriptor: FlightDescriptor):
        """

        NOT USED NOW

        Returning an “access plan” for a dataset of interest, possibly requiring consuming multiple data streams.
        This request can accept custom serialized commands containing, for example, your specific
        application parameters.
        """
        key = UDFServer._descriptor_to_key(descriptor)
        if key in self.flights:
            table = self.flights[key]
            return self._make_flight_info(key, descriptor, table)
        raise KeyError('Flight not found.')

    def do_put(self, context, descriptor: FlightDescriptor, reader, writer):
        """
        Pass Arrow stream from the client to the server. The data must be associated with a `FlightDescriptor`,
        which can be either a path or a command. Here the path is not actually a path on the disk,
        but rather an identifier.
        """
        self.flights[UDFServer._descriptor_to_key(descriptor)] = reader.read_all()

    def do_get(self, context, ticket):
        """
        Before getting the stream, the client must first ask the server for available tickets
        (to the specified dataset) of the specified `FlightDescriptor`.
        """
        key = ast.literal_eval(ticket.ticket.decode())
        if key not in self.flights:
            print("Flight Server:\tNOT IN")
            return None
        return pyarrow.flight.RecordBatchStream(self.flights[key])

    def do_action(self, context, action):
        """
        Each (implementation-specific) action is a string (defined in the script). The client is expected to know
        available actions. When a specific action is called, the server executes the corresponding action and
        maybe will return any results, i.e. a generalized function call.
        """
        if action.type == "healthcheck":
            # to check the status of the server to see if it is running.
            yield self._response(b'Flight Server is up and running!')
        elif action.type == "shutdown":
            # to shutdown the server.
            yield self._response(b'Flight Server is shut down!')
            # Shut down on background thread to avoid blocking current request
            threading.Thread(target=self._delayed_shutdown).start()
        elif action.type == "open":
            # open UDF
            user_args_table = self.flights[self._descriptor_to_key(self._accept(b'args'))]
            self.udf_op.open(*user_args_table.to_pydict()['args'])
            yield self._response(b'Success!')
        elif action.type == "compute":
            # execute UDF
            # prepare input data
            input_key = self._descriptor_to_key(self._accept(b'toPython'))
            try:
                input_table: pyarrow.Table = self.flights[input_key]
                input_dataframe: pandas.DataFrame = input_table.to_pandas()
                output_data_list = []
                # execute and get output data
                for index, row in input_dataframe.iterrows():
                    self.udf_op.accept(row)
                    while self.udf_op.has_next():
                        output_data_list.append(self.udf_op.next())
                output_dataframe = pandas.DataFrame.from_records(output_data_list)
                # send output data to Java
                output_key = self._descriptor_to_key(self._accept(b'fromPython'))
                self.flights[output_key] = pyarrow.Table.from_pandas(output_dataframe)
            except:
                result_buffer = json.dumps({'status': 'Fail', 'errorMessage': traceback.format_exc()})
                yield self._response(result_buffer.encode('utf-8'))

            # discard this batch of input
            self.flights.pop(input_key)

            result_buffer = json.dumps({'status': 'Success'})
            yield self._response(result_buffer.encode('utf-8'))

        elif action.type == "close":
            # close UDF
            self.udf_op.close()
            yield self._response(b'Success!')
        else:
            raise ValueError("Unknown action {!r}".format(action.type))

    def _delayed_shutdown(self):
        """Shut down after a delay."""
        # print("Flight Server:\tServer is shutting down...")

        self.shutdown()
        self.wait()

    @staticmethod
    def _response(message: bytes):
        return pyarrow.flight.Result(pyarrow.py_buffer(message))

    @staticmethod
    def _accept(channel: bytes):
        return pyarrow.flight.FlightDescriptor.for_path(channel)


if __name__ == '__main__':
    _, port, UDF_operator_script_path, *__ = sys.argv
    # Dynamically import operator from user-defined script.

    # Spec is used to load a spec based on a file location (the UDF script)
    spec = importlib.util.spec_from_file_location('user_module', UDF_operator_script_path)
    # Dynamically load the user script as module
    user_module = importlib.util.module_from_spec(spec)
    # Execute the module so that its attributes can be loaded.
    spec.loader.exec_module(user_module)

    # The UDF that will be used in the server. It will be either an inherited operator instance, or created by passing
    # map_func/filter_func to a TexeraMapOperator/TexeraFilterOperator instance.
    final_UDF = None

    if hasattr(user_module, 'operator_instance'):
        final_UDF = user_module.operator_instance
    elif hasattr(user_module, 'map_function'):
        final_UDF = texera_udf_operator_base.TexeraMapOperator(user_module.map_function)
    elif hasattr(user_module, 'filter_function'):
        final_UDF = texera_udf_operator_base.TexeraFilterOperator(user_module.filter_function)
    else:
        raise ValueError("Unsupported UDF definition!")

    location = "grpc+tcp://localhost:" + port

    UDFServer(final_UDF, "localhost", location).serve()
