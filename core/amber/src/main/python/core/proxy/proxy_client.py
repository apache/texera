from typing import Any, Optional

from loguru import logger
from pyarrow import Table
from pyarrow.flight import Action, FlightCallOptions, FlightClient, FlightDescriptor, FlightStreamWriter

from .proxy_server import ProxyServer


class ProxyClient(FlightClient):

    def __init__(self, scheme: str = "grpc+tcp", host: str = "localhost", port: int = 5005, timeout=1000,
                 *args, **kwargs):
        location = f"{scheme}://{host}:{port}"
        super().__init__(location, *args, **kwargs)
        logger.debug("Connected to server at " + location)
        self._timeout = timeout

    @logger.catch(reraise=True)
    def call_action(self, action_name: Any, payload: bytes = bytes()) -> bytes:
        """
        call a specific remote action specified by the name
        :param action_name: the registered action name to be invoked
        :param payload:
        :return: exactly one result in bytes
        """

        action = Action(bytes(action_name, 'utf-8'), payload)
        options = FlightCallOptions(timeout=self._timeout)
        return next(self.do_action(action, options)).body.to_pybytes()

    def send_data(self, command: bytes, table: Optional[Table]) -> None:
        """
        send data to the server
        :param table: a PyArrow.Table of column-stored records.
        :return:
        """
        logger.debug(f"sent command {command}")
        descriptor = FlightDescriptor.for_command(command)
        table = Table.from_arrays([]) if table is None else table
        writer, _ = self.do_put(descriptor, table.schema)
        writer: FlightStreamWriter
        with writer:
            writer.write_table(table)


if __name__ == '__main__':
    with ProxyServer() as server:
        server.register("hello", lambda: "what")
        client = ProxyClient()
        print(client.call_action("hello"))
