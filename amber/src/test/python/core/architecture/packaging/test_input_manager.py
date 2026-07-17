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

import threading

import pyarrow
import pytest

import core.architecture.packaging.input_manager as input_manager_module
from core.architecture.packaging.input_manager import InputManager
from core.models import InternalQueue, Schema
from core.models.payload import DataFrame, DataPayload, StateFrame
from core.models.state import State
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    PortIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import (
    Partitioning,
)

WORKER_ID = "Worker:WF1-test-main-0"


def _channel_id(from_name: str, is_control: bool = False) -> ChannelIdentity:
    return ChannelIdentity(
        ActorVirtualIdentity(from_name),
        ActorVirtualIdentity(WORKER_ID),
        is_control,
    )


class _RecordingReaderRunnable:
    """Stands in for InputPortMaterializationReaderRunnable, whose real
    constructor parses the URI and instantiates a partitioner. Records the
    constructor arguments for assertions."""

    def __init__(self, uri, queue, worker_actor_id, partitioning):
        self.uri = uri
        self.queue = queue
        self.worker_actor_id = worker_actor_id
        self.partitioning = partitioning


class _StubReaderRunnable:
    """A minimal runnable exposing just what start_input_port_mat_reader_threads
    touches: finished(), run(), and channel_id (used in the thread name)."""

    def __init__(self, finished: bool):
        self._finished = finished
        self.channel_id = "stub-channel"
        self.ran = threading.Event()
        self.ran_on_daemon_thread = None

    def finished(self) -> bool:
        return self._finished

    def run(self) -> None:
        self.ran_on_daemon_thread = threading.current_thread().daemon
        self.ran.set()


@pytest.fixture
def input_queue():
    return InternalQueue()


@pytest.fixture
def input_manager(input_queue):
    return InputManager(WORKER_ID, input_queue)


@pytest.fixture
def schema():
    return Schema(raw_schema={"name": "STRING", "age": "INTEGER"})


class TestAddInputPort:
    def test_none_port_fields_are_normalized(self, input_manager, schema):
        port_id = PortIdentity(id=None, internal=None)

        input_manager.add_input_port(port_id, schema, [], [])

        # The passed-in identity is normalized in place, and the port is
        # stored under the normalized key.
        assert port_id.id == 0
        assert port_id.internal is False
        normalized = PortIdentity(id=0, internal=False)
        assert input_manager.get_port(normalized).get_schema() is schema

    def test_re_adding_same_port_keeps_original_worker_port(
        self, input_manager, schema
    ):
        port_id = PortIdentity(id=0, internal=False)
        input_manager.add_input_port(port_id, schema, [], [])
        original_port = input_manager.get_port(port_id)

        other_schema = Schema(raw_schema={"other": "STRING"})
        input_manager.add_input_port(
            PortIdentity(id=0, internal=False), other_schema, [], []
        )

        assert input_manager.get_port(port_id) is original_port
        assert input_manager.get_port(port_id).get_schema() is schema

    def test_none_normalized_port_dedups_against_existing_port(
        self, input_manager, schema, monkeypatch
    ):
        monkeypatch.setattr(
            input_manager_module,
            "InputPortMaterializationReaderRunnable",
            _RecordingReaderRunnable,
        )
        input_manager.add_input_port(
            PortIdentity(id=0, internal=False), schema, ["uri-a"], [Partitioning()]
        )
        original_port = input_manager.get_port(PortIdentity(id=0, internal=False))

        other_schema = Schema(raw_schema={"other": "STRING"})
        input_manager.add_input_port(
            PortIdentity(id=None, internal=None),
            other_schema,
            ["uri-b"],
            [Partitioning()],
        )

        # The None-field identity normalizes to port 0, so this counts as a
        # re-add: the original WorkerPort and its schema are kept ...
        port_id = PortIdentity(id=0, internal=False)
        assert input_manager.get_port(port_id) is original_port
        assert input_manager.get_port(port_id).get_schema() is schema
        # ... but set_up_input_port_mat_reader_threads is called
        # unconditionally, so the port's reader list is still replaced.
        readers = input_manager.get_input_port_mat_reader_threads()[port_id]
        assert [reader.uri for reader in readers] == ["uri-b"]


class TestRegisterInput:
    def test_binds_channel_to_port_in_both_directions(self, input_manager, schema):
        port_id = PortIdentity(id=1, internal=False)
        channel_id = _channel_id("upstream-worker")
        input_manager.add_input_port(port_id, schema, [], [])

        input_manager.register_input(channel_id, port_id)

        assert input_manager.get_port_id(channel_id) == port_id
        assert channel_id in input_manager.get_port(port_id).get_channels()

    def test_none_port_fields_are_normalized(self, input_manager, schema):
        normalized = PortIdentity(id=0, internal=False)
        channel_id = _channel_id("upstream-worker")
        input_manager.add_input_port(PortIdentity(id=0, internal=False), schema, [], [])

        input_manager.register_input(channel_id, PortIdentity(id=None, internal=None))

        assert input_manager.get_port_id(channel_id) == normalized
        assert channel_id in input_manager.get_port(normalized).get_channels()

    def test_re_registering_channel_leaves_stale_reverse_mapping(
        self, input_manager, schema
    ):
        port_a = PortIdentity(id=0, internal=False)
        port_b = PortIdentity(id=1, internal=False)
        channel_id = _channel_id("upstream-worker")
        input_manager.add_input_port(port_a, schema, [], [])
        input_manager.add_input_port(port_b, schema, [], [])

        input_manager.register_input(channel_id, port_a)
        input_manager.register_input(channel_id, port_b)

        # The forward mapping is updated to the new port ...
        assert input_manager.get_port_id(channel_id) == port_b
        assert channel_id in input_manager.get_port(port_b).get_channels()
        # ... but the old port's channel set is never cleaned up, so the
        # channel remains in both reverse mappings (current behavior).
        assert channel_id in input_manager.get_port(port_a).get_channels()


class TestPortCompletion:
    def test_completing_a_channel_marks_only_its_port(self, input_manager, schema):
        port_a = PortIdentity(id=0, internal=False)
        port_b = PortIdentity(id=1, internal=False)
        channel_a = _channel_id("upstream-a")
        channel_b = _channel_id("upstream-b")
        input_manager.add_input_port(port_a, schema, [], [])
        input_manager.add_input_port(port_b, schema, [], [])
        input_manager.register_input(channel_a, port_a)
        input_manager.register_input(channel_b, port_b)

        input_manager.complete_current_port(channel_a)

        assert input_manager.get_port(port_a).completed is True
        assert input_manager.get_port(port_b).completed is False
        assert input_manager.all_ports_completed() is False

        input_manager.complete_current_port(channel_b)

        assert input_manager.all_ports_completed() is True

    def test_all_ports_completed_is_vacuously_true_without_ports(self, input_manager):
        assert input_manager.all_ports_completed() is True

    def test_completing_one_channel_completes_the_whole_port(
        self, input_manager, schema
    ):
        # Completion is tracked per port, not per channel: completing via one
        # channel marks the port done even though a sibling channel on the
        # same port never completed. InputManager does no per-channel
        # counting itself -- this is safe in production because the only
        # caller (end_channel_handler.py) is triggered by an EndChannel ECM
        # sent with EmbeddedControlMessageType.PORT_ALIGNMENT (see
        # start_worker_handler.py), so the handler fires only after all
        # channels of the port have delivered the marker.
        port_id = PortIdentity(id=0, internal=False)
        channel_a = _channel_id("upstream-a")
        channel_b = _channel_id("upstream-b")
        input_manager.add_input_port(port_id, schema, [], [])
        input_manager.register_input(channel_a, port_id)
        input_manager.register_input(channel_b, port_id)

        input_manager.complete_current_port(channel_a)

        assert input_manager.get_port(port_id).completed is True
        assert input_manager.all_ports_completed() is True


class TestChannelIds:
    def test_data_channel_ids_exclude_control_channels(self, input_manager, schema):
        port_id = PortIdentity(id=0, internal=False)
        data_channel = _channel_id("upstream-worker", is_control=False)
        control_channel = _channel_id("controller", is_control=True)
        input_manager.add_input_port(port_id, schema, [], [])
        input_manager.register_input(data_channel, port_id)
        input_manager.register_input(control_channel, port_id)

        assert input_manager.get_all_data_channel_ids() == {data_channel}
        assert set(input_manager.get_all_channel_ids()) == {
            data_channel,
            control_channel,
        }


class TestSetUpInputPortMatReaderThreads:
    def test_mismatched_uris_and_partitionings_raise(self, input_manager):
        with pytest.raises(AssertionError):
            input_manager.set_up_input_port_mat_reader_threads(
                PortIdentity(id=0, internal=False), ["uri-a"], []
            )

    def test_creates_one_reader_per_uri(self, input_manager, input_queue, monkeypatch):
        monkeypatch.setattr(
            input_manager_module,
            "InputPortMaterializationReaderRunnable",
            _RecordingReaderRunnable,
        )
        port_id = PortIdentity(id=0, internal=False)
        partitionings = [Partitioning(), Partitioning()]

        input_manager.set_up_input_port_mat_reader_threads(
            port_id, ["uri-a", "uri-b"], partitionings
        )

        readers = input_manager.get_input_port_mat_reader_threads()[port_id]
        assert [reader.uri for reader in readers] == ["uri-a", "uri-b"]
        assert readers[0].partitioning is partitionings[0]
        assert readers[1].partitioning is partitionings[1]
        for reader in readers:
            assert reader.queue is input_queue
            assert reader.worker_actor_id == ActorVirtualIdentity(WORKER_ID)

    def test_second_set_up_replaces_previous_readers(self, input_manager, monkeypatch):
        monkeypatch.setattr(
            input_manager_module,
            "InputPortMaterializationReaderRunnable",
            _RecordingReaderRunnable,
        )
        port_id = PortIdentity(id=0, internal=False)

        input_manager.set_up_input_port_mat_reader_threads(
            port_id, ["uri-a"], [Partitioning()]
        )
        input_manager.set_up_input_port_mat_reader_threads(
            port_id, ["uri-b"], [Partitioning()]
        )

        # The port's reader list is replaced wholesale, not appended to.
        readers = input_manager.get_input_port_mat_reader_threads()[port_id]
        assert [reader.uri for reader in readers] == ["uri-b"]


class TestStartInputPortMatReaderThreads:
    def test_only_unfinished_readers_are_started(self, input_manager):
        finished_reader = _StubReaderRunnable(finished=True)
        unfinished_reader = _StubReaderRunnable(finished=False)
        input_manager._input_port_mat_reader_runnables[
            PortIdentity(id=0, internal=False)
        ] = [finished_reader, unfinished_reader]

        input_manager.start_input_port_mat_reader_threads()

        assert unfinished_reader.ran.wait(timeout=5)
        # The reader must run off the caller's thread, on a daemon thread.
        assert unfinished_reader.ran_on_daemon_thread is True
        assert not finished_reader.ran.is_set()


class TestProcessDataPayload:
    @pytest.fixture
    def registered_channel(self, input_manager, schema):
        port_id = PortIdentity(id=0, internal=False)
        channel_id = _channel_id("upstream-worker")
        input_manager.add_input_port(port_id, schema, [], [])
        input_manager.register_input(channel_id, port_id)
        return channel_id

    def test_data_frame_yields_tuples_with_port_schema(
        self, input_manager, schema, registered_channel
    ):
        table = pyarrow.Table.from_pydict(
            {"name": ["Alice", "Bob"], "age": [10, 20]},
            schema=schema.as_arrow_schema(),
        )

        results = list(
            input_manager.process_data_payload(
                registered_channel, DataFrame(frame=table)
            )
        )

        assert [tuple_["name"] for tuple_ in results] == ["Alice", "Bob"]
        assert [tuple_["age"] for tuple_ in results] == [10, 20]
        # Each tuple is built against the schema of the channel's port.
        for tuple_ in results:
            assert tuple_._schema is schema

    def test_state_frame_is_passed_through(self, input_manager, registered_channel):
        state = State({"i": 2})

        results = list(
            input_manager.process_data_payload(
                registered_channel, StateFrame(frame=state)
            )
        )

        assert len(results) == 1
        assert results[0] is state

    def test_unknown_payload_type_raises(self, input_manager, registered_channel):
        with pytest.raises(NotImplementedError):
            list(input_manager.process_data_payload(registered_channel, DataPayload()))

    def test_empty_data_frame_yields_no_tuples(
        self, input_manager, schema, registered_channel
    ):
        empty_table = schema.as_arrow_schema().empty_table()

        results = list(
            input_manager.process_data_payload(
                registered_channel, DataFrame(frame=empty_table)
            )
        )

        assert results == []

    def test_state_frame_through_unregistered_channel_passes_through(
        self, input_manager
    ):
        # The StateFrame branch never touches the channel/port tables, so a
        # channel that was never registered still passes state through.
        state = State({"i": 2})

        results = list(
            input_manager.process_data_payload(
                _channel_id("never-registered"), StateFrame(frame=state)
            )
        )

        assert len(results) == 1
        assert results[0] is state

    def test_data_frame_through_unregistered_channel_fails_lazily(
        self, input_manager, schema
    ):
        table = pyarrow.Table.from_pydict(
            {"name": ["Alice"], "age": [10]},
            schema=schema.as_arrow_schema(),
        )

        # process_data_payload is a generator: creating it raises nothing ...
        generator = input_manager.process_data_payload(
            _channel_id("never-registered"), DataFrame(frame=table)
        )
        # ... the unknown-channel lookup fails only upon iteration.
        with pytest.raises(KeyError):
            list(generator)
