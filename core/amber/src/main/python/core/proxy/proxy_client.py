from typing import Optional

from loguru import logger
from pyarrow import Table
from pyarrow.flight import (
    Action,
    FlightCallOptions,
    FlightClient,
    FlightDescriptor,
    FlightStreamWriter,
)


class ProxyClient(FlightClient):
    def __init__(
        self,
        scheme: str = "grpc+tcp",
        host: str = "localhost",
        port: int = 5005,
        timeout=1000,
        *args,
        **kwargs,
    ):
        location = f"{scheme}://{host}:{port}"
        super().__init__(location, *args, **kwargs)
        logger.debug(f"Connected to server at {location}")
        self._timeout = timeout

    @logger.catch(reraise=True)
    def call_action(
        self,
        action_name: str,
        payload: bytes = bytes(),
        options: Optional[FlightCallOptions] = None,
    ) -> bytes:
        """
        Call a specific remote action specified by the name, pass along a payload.
        :param action_name: the registered action name to be invoked.
        :param payload: the action payload in bytes, user should take the
            responsibility to deserialize it.
        :param options: FlightCallOption to config the call.
        :return: exactly one result in bytes.
        """

        action = Action(action_name, payload)
        if options is None:
            options = FlightCallOptions(timeout=self._timeout)
        return next(self.do_action(action, options)).body.to_pybytes()

    @logger.catch(reraise=True)
    def send_data(self, command: bytes, table: Optional[Table]) -> None:
        """
        Send a data batch to the server.
        :param command: a command to in descriptor to pass along, user should take
            the responsibility to deserialize it.
        :param table: a PyArrow.Table of column-stored records.
        """
        descriptor = FlightDescriptor.for_command(command)
        table = Table.from_arrays([]) if table is None else table
        writer, _ = self.do_put(descriptor, table.schema)
        writer: FlightStreamWriter
        with writer:
            writer.write_table(table)
