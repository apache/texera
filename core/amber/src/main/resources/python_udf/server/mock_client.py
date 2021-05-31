from loguru import logger
from pyarrow.flight import Action, FlightCallOptions, FlightClient


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
        action = Action(procedure_name, b'')
        options = FlightCallOptions(timeout=timeout, headers=procedure_kwargs)
        return next(self.do_action(action, options)).body.to_pybytes()

    def list_flights(args, client, connection_args={}):
        pass
        # print('Flights\n=======')
        # for flight in client.list_flights():
        #     descriptor = flight.descriptor
        #     if descriptor.descriptor_type == DescriptorType.PATH:
        #         print("Path:", descriptor.path)
        #     elif descriptor.descriptor_type == DescriptorType.CMD:
        #         print("Command:", descriptor.command)
        #     else:
        #         print("Unknown descriptor type")
        #
        #     print("Total records:", end=" ")
        #     if flight.total_records >= 0:
        #         print(flight.total_records)
        #     else:
        #         print("Unknown")
        #
        #     print("Total bytes:", end=" ")
        #     if flight.total_bytes >= 0:
        #         print(flight.total_bytes)
        #     else:
        #         print("Unknown")
        #
        #     print("Number of endpoints:", len(flight.endpoints))
        #     print("Schema:")
        #     print(flight.schema)
        #     print('---')
        #
        # print('\nActions\n=======')
        # for action in client.list_actions():
        #     print("Type:", action.type)
        #     print("Description:", action.description)
        #     print('---')

    def push_data(args, client, connection_args={}):
        pass
        # print('File Name:', args.file)
        # my_table = csv.read_csv(args.file)
        # print('Table rows=', str(len(my_table)))
        # df = my_table.to_pandas()
        # print(df.head())
        # writer, _ = client.do_put(
        #     FlightDescriptor.for_path(args.file), my_table.schema)
        # writer.write_table(my_table)
        # writer.close()

    def get_flight(args, client, connection_args={}):
        pass
        # if args.path:
        #     descriptor = FlightDescriptor.for_path(*args.path)
        # else:
        #     descriptor = FlightDescriptor.for_command(args.command)
        #
        # info = client.get_flight_info(descriptor)
        # for endpoint in info.endpoints:
        #     print('Ticket:', endpoint.ticket)
        #     for location in endpoint.locations:
        #         print(location)
        #         get_client = FlightClient(location,
        #                                                  **connection_args)
        #         reader = get_client.do_get(endpoint.ticket)
        #         df = reader.read_pandas()
        #         print(df)


if __name__ == '__main__':
    client = UDFMockClient()
    client.call("hello")
