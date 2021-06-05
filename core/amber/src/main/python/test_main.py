from pandas import DataFrame
from pyarrow import Table

from python_rpc import RPCClient
from worker.actor import Actor

if __name__ == '__main__':
    worker1 = Actor(id=1, host="localhost", input_port=5005, output_port=5006)
    worker2 = Actor(id=2, host="localhost", input_port=5006, output_port=5005)

    worker1.start()
    worker2.start()
    df_to_sent = DataFrame({
        'Brand': ['Honda Civic', 'Toyota Corolla', 'Ford Focus', 'Audi A4'],
        'Price': [22000, 25000, 27000, 35000]
    }, columns=['Brand', 'Price'])
    table = Table.from_pandas(df_to_sent)

    client = RPCClient(port=5006)
    # send the pyarrow table to server as a flight
    client.send_data(table, [1])

    worker1.stop()
    worker2.stop()

    worker1.join()
    worker2.join()
