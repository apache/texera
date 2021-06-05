from queue import Queue
from time import sleep

from worker.network_receiver import NetworkReceiver


class TestNetworkReceiver:

    def test_network_receiver_can_stop(self):
        network_thread = NetworkReceiver(Queue(), host="localhost", port=5555)
        network_thread.start()

        sleep(1)
        network_thread.stop()
        network_thread.join()
