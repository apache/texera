import json
from typing import List

from loguru import logger
from pyarrow import Table
from pyarrow.flight import Action, FlightCallOptions, FlightClient
from pyarrow.flight import FlightDescriptor

from .common import serialize_arguments
from .python_rpc_server import PythonRPCServer


class PythonRPCClient(FlightClient):

    def __init__(self, scheme: str = "grpc+tcp", host: str = "localhost", port: int = 5005, timeout=1,
                 *args, **kwargs):
        location = f"{scheme}://{host}:{port}"
        super().__init__(location, *args, **kwargs)
        logger.debug("Connected to server at " + location)
        self._timeout = timeout

    def call(self, procedure_name: str, *procedure_args, **procedure_kwargs):
        """
        call a specific remote procedure specified by the name
        :param procedure_name: the registered procedure name to be invoked
        :param procedure_args, positional arguments for the procedure
        :param procedure_kwargs, keyword arguments for the procedure
        :return: exactly one result in bytes
        """
        payload = serialize_arguments(*procedure_args, **procedure_kwargs)
        action = Action(procedure_name, payload)
        options = FlightCallOptions(timeout=self._timeout)
        return next(self.do_action(action, options)).body.to_pybytes()

    def send_data(self, batch: Table, seq_nums: List[int], on_success: callable = lambda: None, on_error: callable = lambda: None) -> None:
        """
        send data to the server
        :param batch: a PyArrow.Table of column-stored records.
        :param seq_nums: a list of sequence numbers, data identifications for the batch.
        :param on_success: callback function upon success, only used with PythonRPCClient, Java
                            client should not use it.
        :param on_error: callback function upon failure, only used with the PythonRPCClient, Java
                            client should not use it.
        :return:
        """

        try:
            writer, reader = self.do_put(FlightDescriptor.for_path(json.dumps(seq_nums)), batch.schema)
            logger.debug("start writing")
            with writer:
                writer.write_table(batch)

            logger.debug("finish writing")

            # invoke success handler
            on_success()
        except Exception as e:
            logger.warning(e)

            # invoke error handler
            on_error()
            raise


if __name__ == '__main__':
    with PythonRPCServer() as server:
        server.register("hello", lambda: "what")
        client = PythonRPCClient()
        print(client.call("hello"))
