from queue import Queue
from time import sleep

from worker.network_receiver import NetworkReceiver
from worker.network_sender import NetworkSender


class TestNetworkSender:

    def test_network_sender_can_stop(self):
        network_receiver_thread = NetworkReceiver(Queue(), host="localhost", port=5555)
        network_receiver_thread.start()

        network_sender_thread = NetworkSender(Queue(), host="localhost", port=5555)
        network_sender_thread.start()

        sleep(1)
        network_receiver_thread.stop()
        network_sender_thread.stop()
        network_receiver_thread.join()
        network_sender_thread.join()
