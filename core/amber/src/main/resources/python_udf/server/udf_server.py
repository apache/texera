import ast
import json
import os
import threading
from datetime import datetime
from typing import Dict

import pandas
import pyarrow
from loguru import logger
from pyarrow.flight import FlightDescriptor, Action, FlightServerBase, Result, FlightInfo, Location, FlightEndpoint, RecordBatchStream

logger.add(f"texera-python_udf-{datetime.utcnow().isoformat()}-{os.getpid()}.log", rotation="500 MB")


class UDFServer(FlightServerBase):

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
            location = Location.for_grpc_tls(self.host, self.port)
        else:
            location = Location.for_grpc_tcp(self.host, self.port)
        endpoints = [FlightEndpoint(repr(key), [location]), ]

        mock_sink = pyarrow.MockOutputStream()
        stream_writer = pyarrow.RecordBatchStreamWriter(mock_sink, table.schema)
        stream_writer.write_table(table)
        stream_writer.close()
        data_size = mock_sink.size()

        return FlightInfo(table.schema,
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
                descriptor = FlightDescriptor.for_command(key[1])
            else:
                descriptor = FlightDescriptor.for_path(*key[2])

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
            logger.warning("Flight Server:\tNOT IN")
            return None
        return RecordBatchStream(self.flights[key])

    def do_action(self, context, action: Action):
        """
        Each (implementation-specific) action is a string (defined in the script). The client is expected to know
        available actions. When a specific action is called, the server executes the corresponding action and
        maybe will return any results, i.e. a generalized function call.
        """
        logger.debug(f"Flight Server on Action {action.type}")
        if action.type == "health_check":
            # to check the status of the server to see if it is running.
            yield self._response('Flight Server is up and running!')
        elif action.type == "open":

            # set up user configurations
            user_conf_table = self.flights[self._descriptor_to_key(self._to_descriptor('conf'))]
            self._configure(*user_conf_table.to_pydict()['conf'])

            # open UDF
            user_args_table = self.flights[self._descriptor_to_key(self._to_descriptor('args'))]
            self.udf_op.open(*user_args_table.to_pydict()['args'])

            yield self._response('Success!')
        elif action.type == "compute":

            yield self._response(self.compute())


        elif action.type == "input_exhausted":
            self.udf_op.input_exhausted()
            self._output_data()
            yield self._response('Success!')

        elif action.type == "close":
            # close UDF
            self.udf_op.close()
            yield self._response('Success!')

        elif action.type == "terminate":
            # Shut down on background thread to avoid blocking current request
            # this is to be invoked by java end whenever it needs to terminate the server on python end
            threading.Thread(target=self._delayed_shutdown).start()

        else:
            raise ValueError("Unknown action {!r}".format(action.type))

    def _delayed_shutdown(self):
        """Shut down after a delay."""
        logger.debug("Bye bye!")
        self.shutdown()
        self.wait()

    def _output_data(self):
        output_data_list = []
        while self.udf_op.has_next():
            output_data_list.append(self.udf_op.next())
        output_dataframe = pandas.DataFrame.from_records(output_data_list)
        # send output data to Java
        self._send_flight("fromPython", output_dataframe)

    def _get_flight(self, channel: str) -> pandas.DataFrame:
        logger.debug(f"transforming flight {channel.__repr__()}")
        df = self.flights[self._descriptor_to_key(self._to_descriptor(channel))].to_pandas()
        logger.debug(f"got {len(df)} rows in this flight")
        return df

    def _remove_flight(self, channel: str) -> None:
        logger.debug(f"removing flight {channel.__repr__()}")
        self.flights.pop(self._descriptor_to_key(self._to_descriptor(channel)))

    def _send_flight(self, channel: str, output_dataframe: pandas.DataFrame) -> None:
        output_key = self._descriptor_to_key(self._to_descriptor(channel))
        logger.debug(f"prepared {len(output_dataframe)} rows in this flight")
        logger.debug(f"sending flight {channel.__repr__()}")
        self.flights[output_key] = pyarrow.Table.from_pandas(output_dataframe)

    @staticmethod
    def _response(message: str):
        return Result(pyarrow.py_buffer(message.encode()))

    @staticmethod
    def _to_descriptor(channel: str) -> FlightDescriptor:
        return FlightDescriptor.for_path(channel)

    def _configure(self, *args):
        # TODO: add server related configurations here
        pass

    @logger.catch(reraise=False, default=json.dumps({'status': 'Fail'}))
    def compute(self):

        # execute UDF
        # prepare input data
        input_dataframe: pandas.DataFrame = self._get_flight("toPython")

        # execute and output data
        for index, row in input_dataframe.iterrows():
            self.udf_op.accept(row)

        result_buffer = json.dumps({'status': 'Success'})

        self._output_data()
        # discard this batch of input
        self._remove_flight("toPython")
        return result_buffer
