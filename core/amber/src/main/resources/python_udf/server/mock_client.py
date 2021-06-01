import pandas as pd
from loguru import logger
from pyarrow import Schema, Table
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

    def push_data(self, connection_args={}):

        cars = {'Brand': ['Honda Civic', 'Toyota Corolla', 'Ford Focus', 'Audi A4'],
                'Price': [22000, 25000, 27000, 35000]
                }
        df = pd.DataFrame(cars, columns=['Brand', 'Price'])
        writer, _ = self.do_put(FlightDescriptor.for_path("fromClient"), Schema.from_pandas(df))
        table = Table.from_pandas(df)
        writer.write_table(table)
        writer.close()

    def get_flight(args, client, connection_args={}):

        if args.path:
            descriptor = FlightDescriptor.for_path(*args.path)
        else:
            descriptor = FlightDescriptor.for_command(args.command)

        info = client.get_flight_info(descriptor)
        for endpoint in info.endpoints:
            print('Ticket:', endpoint.ticket)
            for location in endpoint.locations:
                print(location)
                get_client = FlightClient(location,
                                          **connection_args)
                reader = get_client.do_get(endpoint.ticket)
                df = reader.read_pandas()
                print(df)


if __name__ == '__main__':
    client = UDFMockClient()
    client.call("hello")
