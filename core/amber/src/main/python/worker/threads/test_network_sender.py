from queue import Queue
from time import sleep

import pytest

from worker.threads.network_receiver import NetworkReceiver
from worker.threads.network_sender import NetworkSender


class TestNetworkSender:
    @pytest.fixture
    def network_receiver(self):
        network_receiver = NetworkReceiver(Queue(), host="localhost", port=5555)
        yield network_receiver
        network_receiver.stop()

    @pytest.fixture
    def network_sender(self):
        network_sender = NetworkSender(Queue(), host="localhost", port=5555)
        yield network_sender
        network_sender.stop()

    @pytest.mark.timeout(0.5)
    def test_network_sender_can_stop(self, network_receiver, network_sender):
        network_receiver.start()
        network_sender.start()
        assert network_receiver.is_alive()
        assert network_sender.is_alive()
        sleep(0.1)
        network_receiver.stop()
        network_sender.stop()
        sleep(0.1)
        assert not network_receiver.is_alive()
        assert not network_sender.is_alive()
        network_receiver.join()
