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

from dataclasses import dataclass

import pytest

from core.models.internal_queue import (
    DataElement,
    DCMElement,
    ECMElement,
    InternalQueue,
    InternalQueueElement,
)
from core.models.payload import DataPayload
from proto.org.apache.texera.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmbeddedControlMessage,
)
from proto.org.apache.texera.amber.engine.common import DirectControlMessagePayloadV2


@dataclass
class UnrecognizedElement(InternalQueueElement):
    """An InternalQueueElement subclass that InternalQueue does not know."""

    pass


class SystemCommand:
    """A non-InternalQueueElement item, routed to the SYSTEM sub-queue."""

    pass


class TestInternalQueue:
    @pytest.fixture
    def queue(self):
        return InternalQueue()

    @pytest.fixture
    def control_channel(self):
        return ChannelIdentity(
            ActorVirtualIdentity("CONTROLLER"),
            ActorVirtualIdentity("dummy_worker_id"),
            True,
        )

    @pytest.fixture
    def data_channel(self):
        return ChannelIdentity(
            ActorVirtualIdentity("upstream_worker_id"),
            ActorVirtualIdentity("dummy_worker_id"),
            False,
        )

    @pytest.fixture
    def second_data_channel(self):
        return ChannelIdentity(
            ActorVirtualIdentity("another_upstream_worker_id"),
            ActorVirtualIdentity("dummy_worker_id"),
            False,
        )

    @staticmethod
    def data_element(channel):
        return DataElement(tag=channel, payload=DataPayload())

    @staticmethod
    def dcm_element(channel):
        return DCMElement(tag=channel, payload=DirectControlMessagePayloadV2())

    @staticmethod
    def ecm_element(channel):
        return ECMElement(tag=channel, payload=EmbeddedControlMessage())

    def test_it_can_init(self, queue):
        assert queue.is_empty()
        assert queue.is_control_empty()
        assert queue.is_data_empty()
        assert queue.size() == 0
        assert len(queue) == 0

    @pytest.mark.timeout(2)
    def test_it_accepts_all_recognized_element_types(
        self, queue, control_channel, data_channel
    ):
        data = self.data_element(data_channel)
        dcm = self.dcm_element(control_channel)
        ecm = self.ecm_element(data_channel)
        # NOTE: LinkedBlockingMultiQueue priority-group ordering is currently
        # dependent on sub-queue registration order; register control before data
        # to preserve control-priority semantics.
        queue.put(dcm)
        queue.put(data)
        queue.put(ecm)
        assert queue.size() == 3
        # the control-channel element goes first, data-channel FIFO after
        assert queue.get() is dcm
        assert queue.get() is data
        assert queue.get() is ecm
        assert queue.is_empty()

    @pytest.mark.timeout(2)
    @pytest.mark.xfail(
        reason=(
            "LinkedBlockingMultiQueue.add_sub_queue does not currently insert new "
            "priority groups ahead of lower-priority ones, so registering data before "
            "control can break control-priority ordering."
        )
    )
    def test_control_elements_dequeue_before_data_even_if_data_channel_registered_first(
        self, queue, control_channel, data_channel
    ):
        data = self.data_element(data_channel)
        dcm = self.dcm_element(control_channel)
        queue.put(data)  # registers the data channel first
        queue.put(dcm)  # registers the control channel later
        assert queue.get() is dcm
        assert queue.get() is data

    @pytest.mark.timeout(2)
    def test_control_elements_dequeue_before_data_elements(
        self, queue, control_channel, data_channel
    ):
        data1 = self.data_element(data_channel)
        data2 = self.data_element(data_channel)
        dcm1 = self.dcm_element(control_channel)
        dcm2 = self.dcm_element(control_channel)
        queue.put(dcm1)
        queue.put(data1)
        queue.put(data2)
        queue.put(dcm2)
        # dcm2 was put last but still dequeues before the earlier data;
        # compare identities since same-payload elements are equal by value
        results = [queue.get() for _ in range(4)]
        assert all(
            got is expected
            for got, expected in zip(results, [dcm1, dcm2, data1, data2])
        )

    @pytest.mark.timeout(2)
    def test_system_elements_dequeue_before_control_and_data(
        self, queue, control_channel, data_channel
    ):
        data = self.data_element(data_channel)
        dcm = self.dcm_element(control_channel)
        system_command = SystemCommand()
        queue.put(dcm)
        queue.put(data)
        queue.put(system_command)
        assert queue.get() is system_command
        assert queue.get() is dcm
        assert queue.get() is data
        assert queue.is_empty()

    def test_it_rejects_unrecognized_internal_queue_elements(self, queue, data_channel):
        with pytest.raises(ValueError, match="not recognized"):
            queue.put(UnrecognizedElement(tag=data_channel))
        # the rejected element must not be enqueued
        assert queue.is_empty()
        assert queue.size() == 0

    @pytest.mark.timeout(2)
    def test_it_maintains_fifo_order_within_a_channel(self, queue, data_channel):
        elements = [self.data_element(data_channel) for _ in range(5)]
        for element in elements:
            queue.put(element)
        results = [queue.get() for _ in range(5)]
        # compare identities: the elements are equal by value, so a plain
        # list equality could not detect a reordering
        assert all(got is put for got, put in zip(results, elements))
        assert queue.is_empty()

    def test_it_reports_emptiness_per_category(
        self, queue, control_channel, data_channel
    ):
        queue.put(self.dcm_element(control_channel))
        assert not queue.is_control_empty()
        assert queue.is_data_empty()
        assert not queue.is_empty()
        queue.put(self.data_element(data_channel))
        assert not queue.is_data_empty()
        queue.get()  # takes the control element
        assert queue.is_control_empty()
        assert not queue.is_data_empty()
        queue.get()  # takes the data element
        assert queue.is_data_empty()
        assert queue.is_empty()

    def test_it_counts_sizes_per_category(
        self, queue, control_channel, data_channel, second_data_channel
    ):
        queue.put(self.data_element(data_channel))
        queue.put(self.data_element(second_data_channel))
        queue.put(self.dcm_element(control_channel))
        assert queue.size_data() == 2
        assert queue.size_control() == 1
        assert queue.size() == 3
        assert len(queue) == 3
        # SYSTEM elements count towards the total but neither category
        queue.put(SystemCommand())
        assert queue.size() == 4
        assert queue.size_data() == 2
        assert queue.size_control() == 1

    @pytest.mark.timeout(2)
    def test_it_can_disable_data_by_pause(self, queue, control_channel, data_channel):
        data = self.data_element(data_channel)
        dcm = self.dcm_element(control_channel)
        queue.put(data)
        queue.put(dcm)
        assert queue.is_data_enabled()
        queue.disable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
        assert not queue.is_data_enabled()
        # only the control element is retrievable; the data element stays
        # queued and still counts towards the data size
        assert queue.get() is dcm
        assert queue.size_data() == 1
        assert queue.enable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
        assert queue.is_data_enabled()
        assert queue.get() is data

    @pytest.mark.timeout(2)
    def test_it_can_disable_data_by_backpressure(self, queue, data_channel):
        data = self.data_element(data_channel)
        queue.put(data)
        queue.disable_data(InternalQueue.DisableType.DISABLE_BY_BACKPRESSURE)
        assert not queue.is_data_enabled()
        assert queue.enable_data(InternalQueue.DisableType.DISABLE_BY_BACKPRESSURE)
        assert queue.is_data_enabled()
        assert queue.get() is data

    @pytest.mark.timeout(2)
    @pytest.mark.parametrize(
        "first_cleared, second_cleared",
        [
            (
                InternalQueue.DisableType.DISABLE_BY_PAUSE,
                InternalQueue.DisableType.DISABLE_BY_BACKPRESSURE,
            ),
            (
                InternalQueue.DisableType.DISABLE_BY_BACKPRESSURE,
                InternalQueue.DisableType.DISABLE_BY_PAUSE,
            ),
        ],
    )
    def test_it_stays_disabled_until_all_reasons_are_cleared(
        self, queue, data_channel, first_cleared, second_cleared
    ):
        data = self.data_element(data_channel)
        queue.put(data)
        queue.disable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
        queue.disable_data(InternalQueue.DisableType.DISABLE_BY_BACKPRESSURE)
        assert not queue.is_data_enabled()
        # clearing one of the two reasons must not re-enable data
        assert not queue.enable_data(first_cleared)
        assert not queue.is_data_enabled()
        # clearing the remaining reason re-enables data
        assert queue.enable_data(second_cleared)
        assert queue.is_data_enabled()
        assert queue.get() is data

    def test_it_can_disable_data_by_the_same_reason_twice(self, queue, data_channel):
        queue.put(self.data_element(data_channel))
        queue.disable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
        queue.disable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
        assert not queue.is_data_enabled()
        # a repeated reason is tracked once, so a single enable clears it
        assert queue.enable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
        assert queue.is_data_enabled()

    def test_it_can_enable_data_by_a_reason_that_was_never_set(
        self, queue, data_channel
    ):
        queue.put(self.data_element(data_channel))
        assert queue.enable_data(InternalQueue.DisableType.DISABLE_BY_BACKPRESSURE)
        assert queue.is_data_enabled()
        # with another reason still set, an unset reason must not re-enable
        queue.disable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
        assert not queue.enable_data(InternalQueue.DisableType.DISABLE_BY_BACKPRESSURE)
        assert not queue.is_data_enabled()

    @pytest.mark.timeout(2)
    def test_it_enqueues_into_an_already_disabled_data_channel(
        self, queue, control_channel, data_channel
    ):
        data_elements = [self.data_element(data_channel) for _ in range(3)]
        dcm = self.dcm_element(control_channel)
        queue.put(dcm)
        queue.put(data_elements[0])  # registers the data channel
        queue.disable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
        # puts into the disabled channel still enqueue
        queue.put(data_elements[1])
        queue.put(data_elements[2])
        assert queue.size_data() == 3
        # control still flows while data is disabled
        assert queue.get() is dcm
        assert queue.enable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
        # all queued data elements come out in FIFO order
        results = [queue.get() for _ in range(3)]
        assert all(got is put for got, put in zip(results, data_elements))
        assert queue.is_empty()

    @pytest.mark.timeout(2)
    def test_it_tracks_in_mem_size_of_data_channels_only(
        self, queue, control_channel, data_channel
    ):
        dcm = self.dcm_element(control_channel)
        system_command = SystemCommand()
        queue.put(dcm)
        queue.put(system_command)
        # control and SYSTEM elements never count towards in_mem_size
        assert queue.in_mem_size() == 0
        # the two data elements have the same in-memory size
        queue.put(self.data_element(data_channel))
        single_element_size = queue.in_mem_size()
        assert single_element_size > 0
        queue.put(self.data_element(data_channel))
        assert queue.in_mem_size() == 2 * single_element_size
        # taking the SYSTEM and control elements changes nothing
        assert queue.get() is system_command
        assert queue.get() is dcm
        assert queue.in_mem_size() == 2 * single_element_size
        # taking the data elements returns the accounting to zero
        queue.get()
        assert queue.in_mem_size() == single_element_size
        queue.get()
        assert queue.in_mem_size() == 0

    @pytest.mark.timeout(2)
    def test_it_can_disable_and_enable_a_single_data_channel(
        self, queue, control_channel, data_channel, second_data_channel
    ):
        # the single-channel pause path used by PauseManager
        dcm = self.dcm_element(control_channel)
        blocked = self.data_element(data_channel)
        flowing = self.data_element(second_data_channel)
        queue.put(dcm)
        queue.put(blocked)
        queue.put(flowing)
        queue.disable(data_channel)
        # control and the other data channel still flow
        assert queue.get() is dcm
        assert queue.get() is flowing
        # the disabled channel's element stays queued; it counts towards
        # size_data but is excluded from the getable size
        assert queue.size_data() == 1
        assert queue.size() == 0
        queue.enable(data_channel)
        assert queue.get() is blocked
        assert queue.is_empty()
