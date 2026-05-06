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

from unittest.mock import MagicMock, patch

import pytest

from core.models import State, StateFrame, Tuple
from core.models.internal_queue import DataElement
from core.models.schema import Schema
from core.storage.runnables.input_port_materialization_reader_runnable import (
    InputPortMaterializationReaderRunnable,
)
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
)


class TestEmitStateWithFilter:
    """Cover the partitioner-filter logic for state payloads in
    InputPortMaterializationReaderRunnable. These tests bypass __init__
    so we don't need a real partitioner or storage URI.
    """

    @pytest.fixture
    def me(self):
        return ActorVirtualIdentity(name="me")

    @pytest.fixture
    def someone_else(self):
        return ActorVirtualIdentity(name="other")

    @pytest.fixture
    def runnable(self, me):
        # __new__ skips __init__ so we can wire only the fields we need.
        instance = InputPortMaterializationReaderRunnable.__new__(
            InputPortMaterializationReaderRunnable
        )
        instance.worker_actor_id = me
        instance.partitioner = MagicMock()
        instance.tuple_schema = Schema(raw_schema={"x": "INTEGER"})
        return instance

    def test_yields_state_frame_for_matching_receiver(self, runnable, me):
        state = State({"k": 1})
        runnable.partitioner.flush_state.return_value = [(me, state)]

        frames = list(runnable.emit_state_with_filter(state))

        assert len(frames) == 1
        assert isinstance(frames[0], StateFrame)
        assert frames[0].frame is state

    def test_filters_out_non_matching_receivers(self, runnable, me, someone_else):
        state = State({"k": 1})
        runnable.partitioner.flush_state.return_value = [
            (someone_else, state),
            (me, state),
            (someone_else, state),
        ]

        frames = list(runnable.emit_state_with_filter(state))

        assert len(frames) == 1
        assert isinstance(frames[0], StateFrame)

    def test_yields_data_frame_for_non_state_payload(self, runnable, me):
        # When the partitioner produces a tuple-batch payload (BroadcastPartitioner
        # case), the runnable must convert it to a DataFrame instead of wrapping
        # it as a StateFrame.
        state = State({"k": 1})
        tuples = [Tuple({"x": 7}, schema=runnable.tuple_schema)]
        runnable.partitioner.flush_state.return_value = [(me, tuples)]

        frames = list(runnable.emit_state_with_filter(state))

        assert len(frames) == 1
        # Should not be wrapped as a StateFrame.
        assert not isinstance(frames[0], StateFrame)
        assert frames[0].frame.num_rows == 1

    def test_empty_partitioner_output_yields_nothing(self, runnable):
        state = State({})
        runnable.partitioner.flush_state.return_value = []

        assert list(runnable.emit_state_with_filter(state)) == []


class TestRunStateReadingBlock:
    """Cover the inner try-block in run() that opens the state document and
    emits its rows as StateFrames.
    """

    @pytest.fixture
    def me(self):
        return ActorVirtualIdentity(name="me")

    @pytest.fixture
    def runnable(self, me):
        instance = InputPortMaterializationReaderRunnable.__new__(
            InputPortMaterializationReaderRunnable
        )
        instance.uri = "vfs:///wf/0/exec/0/result/op-a"
        instance.worker_actor_id = me
        instance.tuple_schema = Schema(raw_schema={"x": "INTEGER"})
        instance._stopped = False
        instance._finished = False
        instance.channel_id = ChannelIdentity(me, me, is_control=False)
        instance.queue = MagicMock()
        instance.partitioner = MagicMock()
        # No tuple-batches and no ECM-flush payloads in these tests.
        instance.partitioner.flush.return_value = []
        return instance

    def test_state_rows_are_emitted_as_state_frames(self, runnable, me):
        state_a = State({"loop_counter": 0})
        state_b = State({"loop_counter": 1})

        # The state document yields opaque tuples; from_tuple deserializes
        # them. Patch from_tuple so we don't have to wire a real serialization.
        result_doc = MagicMock()
        result_doc.get.return_value = iter([])  # No materialized tuples.
        state_doc = MagicMock()
        state_doc.get.return_value = iter(["row-a", "row-b"])

        with (
            patch(
                "core.storage.runnables.input_port_materialization_reader_runnable.DocumentFactory"
            ) as mock_factory,
            patch.object(State, "from_tuple") as mock_from_tuple,
        ):
            mock_factory.open_document.side_effect = [
                (result_doc, runnable.tuple_schema),
                (state_doc, None),
            ]
            mock_from_tuple.side_effect = [state_a, state_b]
            runnable.partitioner.flush_state.side_effect = [
                [(me, state_a)],
                [(me, state_b)],
            ]

            runnable.run()

        # Two StateFrames must have been put on the queue, in order.
        state_frames = [
            call.args[0]
            for call in runnable.queue.put.call_args_list
            if isinstance(call.args[0], DataElement)
            and isinstance(call.args[0].payload, StateFrame)
        ]
        assert [sf.payload.frame for sf in state_frames] == [state_a, state_b]
        assert runnable._finished is True
