import time
from typing import Iterable

from loguru import logger
from pandas import DataFrame
from pyarrow import Table

from python_rpc import RPCClient, RPCServer
from worker import DataTuple
from worker.data_processor import DataProcessor
from worker.udf.udf_operator import UDFOperator

if __name__ == '__main__':
    class EchoOperator(UDFOperator):
        def process_texera_tuple(self, row: DataTuple, nth_child: int = 0) -> Iterable[DataTuple]:
            logger.debug("processing one row")
            return [row]


    server = RPCServer(port=5006)
    server.register_data_handler(lambda x: print(x.to_pandas()))
    with server:
        data_processor = DataProcessor(host="localhost", input_port=5005, output_port=5006, udf_operator=EchoOperator())

        data_processor.start()

        df_to_sent = DataFrame({
            'Brand': ['Honda Civic', 'Toyota Corolla', 'Ford Focus', 'Audi A4'],
            'Price': [22000, 25000, 27000, 35000]
        }, columns=['Brand', 'Price'])
        table = Table.from_pandas(df_to_sent)

        client = RPCClient(port=5005)
        # send the pyarrow table to server as a flight
        client.send_data(table)
        time.sleep(2)

        data_processor.stop()
