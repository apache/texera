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

import pyarrow
import pytest

import core.architecture.packaging.input_manager as input_manager_module
from core.architecture.packaging.input_manager import InputManager
from core.models.payload import DataFrame, StateFrame
from core.models.schema.schema import Schema
from core.models.state import State
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    PortIdentity,
)

WORKER_ID = "Worker:WF0-test-op-main-0"


def _channel(name: str) -> ChannelIdentity:
    return ChannelIdentity(
        ActorVirtualIdentity(name),
        ActorVirtualIdentity(WORKER_ID),
        is_control=False,
    )


def _reader_stub(finished: bool, channel_name: str) -> MagicMock:
    reader = MagicMock()
    reader.finished.return_value = finished
    reader.channel_id = _channel(channel_name)
    return reader


class TestPortIdentityDefaults:
    """PortIdentity arrives from protobuf, where an unset scalar can reach
    Python as None. Both add_input_port and register_input must normalise
    those to the (0, False) defaults *before* the identity is used as a
    dict key -- otherwise the same logical port registered through the two
    paths would hash to two different entries.
    """

    @pytest.fixture
    def manager(self):
        return InputManager(worker_id=WORKER_ID, input_queue=MagicMock())

    def test_add_input_port_defaults_unset_fields(self, manager):
        port_id = PortIdentity(id=None, internal=None)
        schema = Schema(raw_schema={"x": "INTEGER"})

        manager.add_input_port(port_id, schema, [], [])

        assert port_id.id == 0
        assert port_id.internal is False
        # The normalised identity is what got keyed, so a canonical
        # PortIdentity(0, False) resolves to the very same port.
        assert manager.get_port(PortIdentity(0, False)).get_schema() == schema

    def test_register_input_defaults_unset_fields(self, manager):
        canonical = PortIdentity(0, False)
        manager.add_input_port(canonical, Schema(raw_schema={"x": "INTEGER"}), [], [])
        channel_id = _channel("upstream")

        manager.register_input(channel_id, PortIdentity(id=None, internal=None))

        # Registering with the un-normalised identity must attach the
        # channel to the already-added port, not create a second one.
        assert manager.get_port_id(channel_id) == canonical
        assert manager.get_port(canonical).get_channels() == {channel_id}
        assert manager.get_all_data_channel_ids() == {channel_id}

    def test_add_input_port_twice_keeps_the_first_worker_port(self, manager):
        port_id = PortIdentity(0, False)
        first_schema = Schema(raw_schema={"x": "INTEGER"})
        manager.add_input_port(port_id, first_schema, [], [])
        manager.register_input(_channel("upstream"), port_id)

        # A re-add (e.g. a second AddInputChannel for the same port) must
        # not wipe the channels already registered against the port.
        manager.add_input_port(port_id, Schema(raw_schema={"y": "STRING"}), [], [])

        port = manager.get_port(port_id)
        assert port.get_schema() == first_schema
        assert len(port.get_channels()) == 1


class TestInputPortMaterializationReaderThreads:
    @pytest.fixture
    def manager(self):
        return InputManager(worker_id=WORKER_ID, input_queue=MagicMock())

    def test_reader_runnables_are_built_per_uri_and_exposed(self, manager):
        port_id = PortIdentity(0, False)
        partitioning_a = MagicMock(name="partitioning-a")
        partitioning_b = MagicMock(name="partitioning-b")

        with patch.object(
            input_manager_module, "InputPortMaterializationReaderRunnable"
        ) as runnable_cls:
            manager.add_input_port(
                port_id,
                Schema(raw_schema={"x": "INTEGER"}),
                ["vfs:///a", "vfs:///b"],
                [partitioning_a, partitioning_b],
            )

        # One runnable per (uri, partitioning) pair, wired to this
        # worker's shared input queue.
        assert runnable_cls.call_count == 2
        assert [call.kwargs["uri"] for call in runnable_cls.call_args_list] == [
            "vfs:///a",
            "vfs:///b",
        ]
        assert [
            call.kwargs["partitioning"] for call in runnable_cls.call_args_list
        ] == [
            partitioning_a,
            partitioning_b,
        ]
        assert runnable_cls.call_args.kwargs["worker_actor_id"] == ActorVirtualIdentity(
            WORKER_ID
        )
        assert runnable_cls.call_args.kwargs["queue"] is manager._input_queue

        registered = manager.get_input_port_mat_reader_threads()
        assert list(registered) == [port_id]
        assert len(registered[port_id]) == 2

    def test_no_uris_registers_an_empty_runnable_list(self, manager):
        port_id = PortIdentity(0, False)

        manager.add_input_port(port_id, Schema(raw_schema={"x": "INTEGER"}), [], [])

        assert manager.get_input_port_mat_reader_threads() == {port_id: []}

    def test_mismatched_uris_and_partitionings_are_rejected(self, manager):
        with pytest.raises(AssertionError):
            manager.set_up_input_port_mat_reader_threads(
                PortIdentity(0, False), ["vfs:///a"], []
            )

    def test_start_threads_skips_already_finished_readers(self, manager):
        # A reader that already drained its materialized input must not be
        # replayed: restarting it would re-emit every tuple plus a second
        # StartChannel/EndChannel pair on the same channel.
        pending = _reader_stub(finished=False, channel_name="pending")
        drained = _reader_stub(finished=True, channel_name="drained")
        manager._input_port_mat_reader_runnables[PortIdentity(0, False)] = [
            pending,
            drained,
        ]

        with patch.object(input_manager_module, "threading") as threading_mock:
            manager.start_input_port_mat_reader_threads()

        threading_mock.Thread.assert_called_once_with(
            target=pending.run,
            daemon=True,
            name=f"port_mat_reader_runnable_thread_{pending.channel_id}",
        )
        # Daemon threads are started, never joined, by this call.
        threading_mock.Thread.return_value.start.assert_called_once_with()

    def test_start_threads_covers_every_port(self, manager):
        first = _reader_stub(finished=False, channel_name="first")
        second = _reader_stub(finished=False, channel_name="second")
        manager._input_port_mat_reader_runnables[PortIdentity(0, False)] = [first]
        manager._input_port_mat_reader_runnables[PortIdentity(1, False)] = [second]

        with patch.object(input_manager_module, "threading") as threading_mock:
            manager.start_input_port_mat_reader_threads()

        started_targets = {
            call.kwargs["target"] for call in threading_mock.Thread.call_args_list
        }
        assert started_targets == {first.run, second.run}


class TestProcessDataPayload:
    @pytest.fixture
    def manager(self):
        manager = InputManager(worker_id=WORKER_ID, input_queue=MagicMock())
        manager.add_input_port(
            PortIdentity(0, False), Schema(raw_schema={"x": "INTEGER"}), [], []
        )
        manager.register_input(_channel("upstream"), PortIdentity(0, False))
        return manager

    def test_unknown_payload_type_is_rejected(self, manager):
        channel_id = _channel("upstream")

        # An unrecognised DataPayload must fail loudly rather than be
        # silently dropped -- the tuple stream would go missing otherwise.
        with pytest.raises(NotImplementedError):
            list(manager.process_data_payload(channel_id, MagicMock(name="payload")))

    def test_unknown_payload_type_still_records_the_current_channel(self, manager):
        channel_id = _channel("upstream")

        with pytest.raises(NotImplementedError):
            list(manager.process_data_payload(channel_id, object()))

        assert manager._current_channel_id == channel_id

    def test_state_frame_envelope_is_forwarded_untouched(self, manager):
        channel_id = _channel("upstream")
        state_frame = StateFrame(frame=State({"i": 1}), loop_counter=3)

        outputs = list(manager.process_data_payload(channel_id, state_frame))

        # The whole envelope is yielded (not just .frame) so the runtime can
        # read loop_counter off it.
        assert outputs == [state_frame]
        assert outputs[0].loop_counter == 3

    def test_data_frame_is_expanded_into_schema_bearing_tuples(self, manager):
        channel_id = _channel("upstream")
        schema = Schema(raw_schema={"x": "INTEGER"})
        table = pyarrow.Table.from_pydict(
            {"x": [1, 2]}, schema=schema.as_arrow_schema()
        )

        outputs = list(manager.process_data_payload(channel_id, DataFrame(frame=table)))

        assert [t["x"] for t in outputs] == [1, 2]
        assert all(t._schema == schema for t in outputs)


class TestPortCompletion:
    def test_ports_complete_independently(self):
        manager = InputManager(worker_id=WORKER_ID, input_queue=MagicMock())
        port_a, port_b = PortIdentity(0, False), PortIdentity(1, False)
        channel_a, channel_b = _channel("a"), _channel("b")
        for port_id, channel_id in ((port_a, channel_a), (port_b, channel_b)):
            manager.add_input_port(port_id, Schema(raw_schema={"x": "INTEGER"}), [], [])
            manager.register_input(channel_id, port_id)

        assert manager.all_ports_completed() is False

        manager.complete_current_port(channel_a)
        assert manager.all_ports_completed() is False

        manager.complete_current_port(channel_b)
        assert manager.all_ports_completed() is True
        assert set(manager.get_all_channel_ids()) == {channel_a, channel_b}
