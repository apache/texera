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
from loguru import logger

from core.models import DataFrame, State, StateFrame, Tuple
from core.models.internal_queue import DataElement, ECMElement
from core.models.schema import Schema
from core.storage.runnables.input_port_materialization_reader_runnable import (
    InputPortMaterializationReaderRunnable,
)
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
)

DOCUMENT_FACTORY = (
    "core.storage.runnables.input_port_materialization_reader_runnable.DocumentFactory"
)


def _build_reader(worker_actor_id, schema=None):
    """Build a reader without running __init__ (which would resolve a real
    partitioner and a real storage URI). Mirrors the fixture in
    TestRunStateReadingBlock, but parameterised so the tuple-loop tests can
    drive the partitioner themselves.
    """
    reader = InputPortMaterializationReaderRunnable.__new__(
        InputPortMaterializationReaderRunnable
    )
    reader.uri = "vfs:///wf/0/exec/0/result/op-a"
    reader.worker_actor_id = worker_actor_id
    reader.tuple_schema = (
        schema if schema is not None else Schema(raw_schema={"x": "INTEGER"})
    )
    reader._stopped = False
    reader._finished = False
    reader.channel_id = ChannelIdentity(
        worker_actor_id, worker_actor_id, is_control=False
    )
    reader.queue = MagicMock()
    reader.partitioner = MagicMock()
    reader.partitioner.flush.return_value = []
    return reader


def _emitted_data_frames(reader):
    return [
        call.args[0].payload
        for call in reader.queue.put.call_args_list
        if isinstance(call.args[0], DataElement)
        and isinstance(call.args[0].payload, DataFrame)
    ]


def _emitted_ecm_names(reader):
    return [
        call.args[0].payload.id.id
        for call in reader.queue.put.call_args_list
        if isinstance(call.args[0], ECMElement)
    ]


class TestRunStateReadingBlock:
    """Cover the state-reading block in run() that opens the state
    document and emits its rows as StateFrames directly to the input
    queue (no partitioner filtering -- state is broadcast to every
    worker).
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

    def test_state_rows_are_emitted_as_state_frames(self, runnable):
        state_a = State({"i": 0})
        state_b = State({"i": 1})

        # The state document yields opaque multi-column tuples. State.from_tuple
        # (patched) deserializes the content column; the reader reads the
        # loop-control columns directly off the row and carries them onto the
        # emitted StateFrame envelope.
        row_a = {
            State.LOOP_COUNTER: 0,
            State.LOOP_START_ID: "loop-a",
        }
        row_b = {
            State.LOOP_COUNTER: 1,
            State.LOOP_START_ID: "loop-b",
        }
        result_doc = MagicMock()
        result_doc.get.return_value = iter([])  # No materialized tuples.
        state_doc = MagicMock()
        state_doc.get.return_value = iter([row_a, row_b])

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

            runnable.run()

        # Two StateFrames must have been put on the queue, in order.
        # The state replay must NOT route through the partitioner --
        # state is shared context, broadcast to every worker.
        runnable.partitioner.flush_state.assert_not_called()
        state_frames = [
            call.args[0]
            for call in runnable.queue.put.call_args_list
            if isinstance(call.args[0], DataElement)
            and isinstance(call.args[0].payload, StateFrame)
        ]
        assert [sf.payload.frame for sf in state_frames] == [state_a, state_b]
        assert [sf.payload.loop_counter for sf in state_frames] == [0, 1]
        assert [sf.payload.loop_start_id for sf in state_frames] == ["loop-a", "loop-b"]
        assert runnable._finished is True


class TestTupleToBatchWithFilter:
    """The reader reuses the *output*-side partitioner to decide which
    tuples this worker would have received, then keeps only its own
    share. Batches addressed to the other (hypothetical) downstream
    workers must be dropped, otherwise a shuffled materialized port would
    replay every partition into every worker.
    """

    @pytest.fixture
    def me(self):
        return ActorVirtualIdentity(name="me")

    def test_only_batches_addressed_to_this_worker_are_yielded(self, me):
        other = ActorVirtualIdentity(name="other")
        reader = _build_reader(me)
        reader.partitioner.add_tuple_to_batch.return_value = [
            (other, [Tuple({"x": 9})]),
            (me, [Tuple({"x": 1}), Tuple({"x": 2})]),
            (other, [Tuple({"x": 8})]),
        ]

        frames = list(reader.tuple_to_batch_with_filter(Tuple({"x": 0})))

        assert len(frames) == 1
        assert frames[0].frame.to_pydict() == {"x": [1, 2]}

    def test_no_batch_for_this_worker_yields_nothing(self, me):
        other = ActorVirtualIdentity(name="other")
        reader = _build_reader(me)
        reader.partitioner.add_tuple_to_batch.return_value = [
            (other, [Tuple({"x": 9})]),
        ]

        assert list(reader.tuple_to_batch_with_filter(Tuple({"x": 0}))) == []

    def test_tuples_to_data_frame_projects_the_schema_column_order(self, me):
        # Column order and arrow types come from the tuple schema, not from
        # the tuple's own field order -- the downstream arrow reader relies
        # on that alignment.
        schema = Schema(raw_schema={"x": "INTEGER", "y": "STRING"})
        reader = _build_reader(me, schema=schema)

        frame = reader.tuples_to_data_frame(
            [Tuple({"y": "a", "x": 1}), Tuple({"y": "b", "x": 2})]
        )

        assert frame.frame.column_names == ["x", "y"]
        assert frame.frame.to_pydict() == {"x": [1, 2], "y": ["a", "b"]}
        assert frame.frame.schema == schema.as_arrow_schema()


class TestRunTupleLoop:
    """Cover the tuple-replay half of run(): each stored tuple is cast to
    the port schema, routed through the partitioner filter, and emitted as
    a DataFrame between the StartChannel and EndChannel ECMs.
    """

    @pytest.fixture
    def me(self):
        return ActorVirtualIdentity(name="me")

    @staticmethod
    def _run_with_documents(reader, tuples, states=()):
        result_doc = MagicMock()
        result_doc.get.return_value = iter(tuples)
        state_doc = MagicMock()
        state_doc.get.return_value = iter(states)
        with patch(DOCUMENT_FACTORY) as mock_factory:
            mock_factory.open_document.side_effect = [
                (result_doc, reader.tuple_schema),
                (state_doc, None),
            ]
            reader.run()
        return result_doc

    def test_tuples_are_cast_to_the_port_schema_before_emission(self, me):
        reader = _build_reader(me)
        # A materialized INT column can come back as a float (pandas
        # promotes null-holding int columns to float64). Without the
        # cast_to_schema() call the arrow conversion below would not
        # produce ints.
        reader.partitioner.add_tuple_to_batch.side_effect = lambda tup: [(me, [tup])]

        self._run_with_documents(reader, [Tuple({"x": 1.0}), Tuple({"x": 2})])

        frames = _emitted_data_frames(reader)
        assert [frame.frame.to_pydict() for frame in frames] == [
            {"x": [1]},
            {"x": [2]},
        ]
        assert all(
            isinstance(value, int)
            for frame in frames
            for value in frame.frame.column("x").to_pylist()
        )
        assert reader.finished() is True

    def test_channel_is_bracketed_by_start_and_end_ecms(self, me):
        reader = _build_reader(me)
        # Make the partitioner behave like the real one: flush hands back
        # the ECM so it reaches the queue.
        reader.partitioner.flush.side_effect = lambda receiver, ecm: [ecm]
        reader.partitioner.add_tuple_to_batch.side_effect = lambda tup: [(me, [tup])]

        self._run_with_documents(reader, [Tuple({"x": 1})])

        assert _emitted_ecm_names(reader) == ["StartChannel", "EndChannel"]
        # The data must land between the two ECMs.
        payload_kinds = [
            type(call.args[0]).__name__ for call in reader.queue.put.call_args_list
        ]
        assert payload_kinds == ["ECMElement", "DataElement", "ECMElement"]

    def test_stop_halts_replay_but_still_closes_the_channel(self, me):
        reader = _build_reader(me)
        reader.partitioner.flush.side_effect = lambda receiver, ecm: [ecm]
        reader.partitioner.add_tuple_to_batch.side_effect = lambda tup: [(me, [tup])]
        state_row = {State.LOOP_COUNTER: 0, State.LOOP_START_ID: ""}

        reader.stop()
        with patch.object(State, "from_tuple", return_value=State({"i": 1})) as load:
            self._run_with_documents(
                reader, [Tuple({"x": 1}), Tuple({"x": 2})], [state_row]
            )

        # No state or tuple is replayed once stopped ...
        load.assert_not_called()
        assert _emitted_data_frames(reader) == []
        reader.partitioner.add_tuple_to_batch.assert_not_called()
        # ... but the channel is still closed, so the downstream port
        # alignment does not hang waiting for an EndChannel.
        assert _emitted_ecm_names(reader) == ["StartChannel", "EndChannel"]
        assert reader.finished() is True

    def test_stop_sets_the_flag_before_run(self, me):
        reader = _build_reader(me)
        assert reader._stopped is False

        reader.stop()

        assert reader._stopped is True


class TestRunErrorHandling:
    """run() executes on a detached daemon thread, so it must swallow and
    log failures instead of propagating them into an unhandled thread
    exception. A reader that failed must not report itself as finished --
    otherwise start_input_port_mat_reader_threads would skip retrying it.
    """

    @pytest.fixture
    def me(self):
        return ActorVirtualIdentity(name="me")

    def test_open_document_failure_is_logged_and_leaves_reader_unfinished(self, me):
        reader = _build_reader(me)
        messages = []
        handler_id = logger.add(messages.append, level="ERROR")
        try:
            with patch(DOCUMENT_FACTORY) as mock_factory:
                mock_factory.open_document.side_effect = RuntimeError(
                    "catalog unreachable"
                )
                reader.run()  # must not raise
        finally:
            logger.remove(handler_id)

        assert reader.finished() is False
        reader.queue.put.assert_not_called()
        assert any("catalog unreachable" in str(message) for message in messages)

    def test_failure_midway_leaves_the_reader_unfinished(self, me):
        reader = _build_reader(me)
        reader.partitioner.add_tuple_to_batch.side_effect = RuntimeError(
            "bad partition"
        )
        result_doc = MagicMock()
        result_doc.get.return_value = iter([Tuple({"x": 1})])
        state_doc = MagicMock()
        state_doc.get.return_value = iter([])

        handler_id = logger.add(lambda _: None, level="ERROR")
        try:
            with patch(DOCUMENT_FACTORY) as mock_factory:
                mock_factory.open_document.side_effect = [
                    (result_doc, reader.tuple_schema),
                    (state_doc, None),
                ]
                reader.run()
        finally:
            logger.remove(handler_id)

        # The EndChannel is never reached, so the reader stays unfinished.
        assert reader.finished() is False
        assert _emitted_ecm_names(reader) == []
