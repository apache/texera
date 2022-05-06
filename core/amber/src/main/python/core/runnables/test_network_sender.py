import threading
from time import sleep

import pytest

from core.models.internal_queue import InternalQueue
from core.runnables.network_receiver import NetworkReceiver
from core.runnables.network_sender import NetworkSender


class TestNetworkSender:
    @pytest.fixture
    def network_receiver(self):
        network_receiver = NetworkReceiver(InternalQueue(), host="localhost", port=5555)
        yield network_receiver
        network_receiver.stop()

    @pytest.fixture
    def network_receiver_thread(self, network_receiver):
        network_receiver_thread = threading.Thread(target=network_receiver.run)
        yield network_receiver_thread

    @pytest.fixture
    def network_sender(self):
        network_sender = NetworkSender(InternalQueue(), host="localhost", port=5555)
        yield network_sender
        network_sender.stop()

    @pytest.fixture
    def network_sender_thread(self, network_sender):
        network_sender_thread = threading.Thread(target=network_sender.run)
        yield network_sender_thread

    @pytest.mark.timeout(2)
    def test_network_sender_can_stop(
        self,
        network_receiver,
        network_receiver_thread,
        network_sender,
        network_sender_thread,
    ):
        network_receiver_thread.start()
        network_sender_thread.start()
        assert network_receiver_thread.is_alive()
        assert network_sender_thread.is_alive()
        sleep(0.1)
        network_receiver.stop()
        network_sender.stop()
        sleep(0.1)
        assert not network_receiver_thread.is_alive()
        assert not network_sender_thread.is_alive()
        network_receiver_thread.join()
        network_sender_thread.join()
