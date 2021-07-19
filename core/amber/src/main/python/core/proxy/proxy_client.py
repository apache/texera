from loguru import logger
from pyarrow import Table
from pyarrow.flight import Action, FlightCallOptions, FlightClient, FlightDescriptor, FlightStreamWriter
from typing import Optional

from .common import serialize_arguments
from .proxy_server import ProxyServer


class ProxyClient(FlightClient):

    def __init__(self, scheme: str = "grpc+tcp", host: str = "localhost", port: int = 5005, timeout=1000,
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
        if procedure_name == "control":
            action = Action(procedure_name, *procedure_args)
        else:
            payload = serialize_arguments(*procedure_args, **procedure_kwargs)
            action = Action(procedure_name, payload)
        options = FlightCallOptions(timeout=self._timeout)
        return next(self.do_action(action, options)).body.to_pybytes()

    def send_data(self, command: bytes, table: Optional[Table]) -> None:
        """
        send data to the server
        :param table: a PyArrow.Table of column-stored records.
        :return:
        """

        descriptor = FlightDescriptor.for_command(command)
        table = Table.from_arrays([]) if table is None else table

        refs = self.do_put(descriptor, table.schema)
        writer: FlightStreamWriter = refs[0]
        try:
            with writer:
                writer.write_table(table)
        except Exception as err:
            logger.exception(err)


if __name__ == '__main__':
    with ProxyServer() as server:
        server.register("hello", lambda: "what")
        client = ProxyClient()
        print(client.call("hello"))
