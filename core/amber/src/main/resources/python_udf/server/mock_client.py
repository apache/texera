from loguru import logger
from pyarrow import Table
from pyarrow.flight import Action, FlightCallOptions, FlightClient
from pyarrow.flight import FlightDescriptor


class UDFMockClient(FlightClient):

    def __init__(self, host: str = "localhost", port: int = 5005, scheme: str = "grpc+tcp", *args, **kwargs):
        location = f"{scheme}://{host}:{port}"
        super().__init__(location, *args, **kwargs)
        logger.info("Connected to server at " + location)

    def call(self, procedure_name: str, timeout: int = 1, **procedure_kwargs):
        """
        call a specific remote procedure specified by the name
        :param procedure_name: the registered procedure name to be invoked
        :param timeout: in seconds
        :return: exactly one result in bytes
        """
        action = Action(procedure_name, b'1')
        options = FlightCallOptions(timeout=timeout, headers=procedure_kwargs)
        return next(self.do_action(action, options)).body.to_pybytes()

    def send_data(self, table: Table, on_success: callable = lambda: None, on_error: callable = lambda: None):
        try:
            writer, reader = self.do_put(FlightDescriptor.for_path("fromClient"), table.schema)
            logger.debug("start writing")
            writer.write_table(table)
            writer.close()
            logger.debug("finish writing")

            # invoke success handler
            on_success()
        except Exception as e:
            logger.exception(e)

            # invoke error handler
            on_error()


if __name__ == '__main__':
    client = UDFMockClient()
    client.call("hello")
