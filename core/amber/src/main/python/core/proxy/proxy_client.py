from typing import Optional

from loguru import logger
from pyarrow import Table
from pyarrow.flight import Action, FlightCallOptions, FlightClient, FlightDescriptor, FlightStreamWriter

from .common import serialize_arguments
from .proxy_server import ProxyServer


class ProxyClient(FlightClient):

    def __init__(self, scheme: str = "grpc+tcp", host: str = "localhost", port: int = 5005, timeout=1000,
                 *args, **kwargs):
        location = f"{scheme}://{host}:{port}"
        super().__init__(location, *args, **kwargs)
        logger.debug("Connected to server at " + location)
        self._timeout = timeout

    def call_action(self, action_name: str, *action_args, **action_kwargs) -> bytes:
        """
        call a specific remote action specified by the name
        :param action_name: the registered action name to be invoked
        :param action_args, positional arguments for the action
        :param action_kwargs, keyword arguments for the action
        :return: exactly one result in bytes
        """
        if action_name == "control":
            action = Action(action_name, *action_args)
        else:
            payload = serialize_arguments(*action_args, **action_kwargs)
            action = Action(action_name, payload)
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
        writer, _ = self.do_put(descriptor, table.schema)
        writer: FlightStreamWriter
        with writer:
            writer.write_table(table)


if __name__ == '__main__':
    with ProxyServer() as server:
        server.register("hello", lambda: "what")
        client = ProxyClient()
        print(client.call_action("hello"))
