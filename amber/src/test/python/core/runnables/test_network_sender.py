# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest
import threading
from contextlib import contextmanager
from time import sleep

from loguru import logger

from core.models.internal_queue import InternalQueue, InternalQueueElement
from core.models.payload import DataFrame
from core.runnables.network_receiver import NetworkReceiver
from core.runnables.network_sender import NetworkSender
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
)


@contextmanager
def muted_catch_logs():
    """
    `NetworkSender._send_data` is wrapped in `@logger.catch(reraise=True)`,
    which logs the whole traceback at ERROR before re-raising. CI runs pytest
    with `-s` and `LOGURU_LEVEL=WARNING`, so an expected failure would
    otherwise dump a traceback into the build log and read as a real error.

    `logger.catch` attributes the record to the *caller's* module rather than
    to the decorated function's module, so the name to silence is this test
    module's own `__name__` (which varies with pytest's import mode -- hence
    `__name__` rather than a literal).
    """
    logger.disable(__name__)
    try:
        yield
    finally:
        logger.enable(__name__)


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

    @pytest.fixture
    def channel_id(self):
        worker_id = ActorVirtualIdentity(name="test")
        return ChannelIdentity(worker_id, worker_id, False)

    @pytest.mark.timeout(5)
    def test_receive_rejects_an_element_that_is_neither_data_control_nor_ecm(
        self, network_sender, channel_id
    ):
        # A plain subclass of InternalQueueElement is neither DataElement nor
        # DCMElement nor ECMElement, so it walks the whole dispatch chain and
        # falls off the end. The sender must refuse it loudly rather than drop
        # it silently.
        #
        # The stable `__repr__` lets the matcher pin the *interpolated* entry
        # as well, mirroring the payload test below: matching only the constant
        # prefix would leave `{next_entry}` -- the sole non-constant part of
        # that line -- unconstrained.
        class UnknownEntry(InternalQueueElement):
            def __repr__(self):
                return "<unknown-entry>"

        unknown = UnknownEntry(tag=channel_id)
        with pytest.raises(TypeError, match="Unexpected entry <unknown-entry>"):
            network_sender.receive(unknown)

    @pytest.mark.timeout(5)
    def test_send_data_rejects_a_payload_that_is_neither_dataframe_nor_stateframe(
        self, network_sender, channel_id
    ):
        class NotAPayload:
            def __repr__(self):
                return "<not-a-payload>"

        with muted_catch_logs():
            with pytest.raises(TypeError, match="Unexpected payload <not-a-payload>"):
                network_sender._send_data(channel_id, NotAPayload())

    @pytest.mark.timeout(5)
    def test_receive_routes_a_data_element_to_send_data(
        self, network_sender, channel_id, monkeypatch
    ):
        # Guards the dispatch chain above: without a positive case, swapping
        # the DataElement arm for the else-arm would still leave the negative
        # test green.
        from core.models.internal_queue import DataElement

        seen = []
        monkeypatch.setattr(
            network_sender,
            "_send_data",
            lambda to, payload: seen.append((to, payload)),
        )
        payload = DataFrame(frame=None)
        network_sender.receive(DataElement(tag=channel_id, payload=payload))
        assert seen == [(channel_id, payload)]
