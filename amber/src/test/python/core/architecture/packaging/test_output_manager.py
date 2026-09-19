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

from core.architecture.packaging.output_manager import OutputManager
from core.architecture.sendsemantics.broad_cast_partitioner import BroadcastPartitioner
from core.architecture.sendsemantics.hash_based_shuffle_partitioner import (
    HashBasedShufflePartitioner,
)
from core.architecture.sendsemantics.one_to_one_partitioner import OneToOnePartitioner
from core.architecture.sendsemantics.range_based_shuffle_partitioner import (
    RangeBasedShufflePartitioner,
)
from core.architecture.sendsemantics.round_robin_partitioner import (
    RoundRobinPartitioner,
)
from core.models import Schema, Tuple
from core.models.payload import DataFrame, StateFrame
from core.models.state import State
from core.storage.runnables.port_storage_writer import PortStorageWriterElement
from core.util import set_one_of
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    PhysicalLink,
    PortIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmbeddedControlMessage,
)
from proto.org.apache.texera.amber.engine.architecture.sendsemantics import (
    BroadcastPartitioning,
    HashBasedShufflePartitioning,
    OneToOnePartitioning,
    Partitioning,
    RangeBasedShufflePartitioning,
    RoundRobinPartitioning,
)

_WORKER_ID = "Worker:WF0-test-main-0"


def _worker(name: str) -> ActorVirtualIdentity:
    return ActorVirtualIdentity(name=name)


def _self_channel(dst: str) -> ChannelIdentity:
    """A data channel whose sender is this OutputManager's own worker."""
    return ChannelIdentity(
        from_worker_id=_worker(_WORKER_ID), to_worker_id=_worker(dst)
    )


def _stub_state_writer(output_manager, port_id):
    """Inject a (queue, writer, thread) triple as if a port were set up."""
    queue = MagicMock()
    writer = MagicMock()
    thread = MagicMock()
    output_manager._port_state_writers[port_id] = (queue, writer, thread)
    return queue, writer, thread


def _stub_tuple_writer(output_manager, port_id):
    """Inject a (queue, writer, thread) triple as if a port were set up."""
    queue = MagicMock()
    writer = MagicMock()
    thread = MagicMock()
    output_manager._port_storage_writers[port_id] = (queue, writer, thread)
    return queue, writer, thread


class TestSaveStateToStorageIfNeeded:
    @pytest.fixture
    def output_manager(self):
        return OutputManager(worker_id="Worker:WF0-test-main-0")

    @pytest.fixture
    def port_a(self):
        return PortIdentity(id=0, internal=False)

    @pytest.fixture
    def port_b(self):
        return PortIdentity(id=1, internal=False)

    @pytest.fixture
    def state(self):
        return State({"i": 2})

    def test_no_state_writers_is_a_noop(self, output_manager, state):
        # With no port set up, save_state_to_storage_if_needed must not
        # touch any writer.
        output_manager.save_state_to_storage_if_needed(state, 0)  # no-op

    def test_unknown_port_id_is_a_noop(self, output_manager, state, port_a):
        output_manager.save_state_to_storage_if_needed(state, 0, port_id=port_a)
        # No assertion needed -- the absence of any writer means nothing
        # was attempted.

    def test_enqueues_to_every_port_when_port_id_omitted(
        self, output_manager, state, port_a, port_b
    ):
        queue_a, _, _ = _stub_state_writer(output_manager, port_a)
        queue_b, _, _ = _stub_state_writer(output_manager, port_b)

        output_manager.save_state_to_storage_if_needed(state, 0)

        # Each port's writer queue receives one PortStorageWriterElement.
        # Critically, save is non-blocking -- the call must not invoke
        # put_one / close on the buffered writer directly (those happen
        # off-thread).
        assert queue_a.put.call_count == 1
        assert queue_b.put.call_count == 1
        assert isinstance(queue_a.put.call_args.args[0], PortStorageWriterElement)
        assert isinstance(queue_b.put.call_args.args[0], PortStorageWriterElement)

    def test_enqueues_only_to_selected_port_when_port_id_specified(
        self, output_manager, state, port_a, port_b
    ):
        queue_a, _, _ = _stub_state_writer(output_manager, port_a)
        queue_b, _, _ = _stub_state_writer(output_manager, port_b)

        output_manager.save_state_to_storage_if_needed(state, 0, port_id=port_a)

        assert queue_a.put.call_count == 1
        queue_b.put.assert_not_called()

    def test_close_port_storage_writers_stops_state_threads(
        self, output_manager, port_a, port_b
    ):
        # After the port completes, every state-writer thread must be
        # stopped and joined so the buffered writer's close() (which
        # flushes the final Iceberg commit) actually runs.
        _, writer_a, thread_a = _stub_state_writer(output_manager, port_a)
        _, writer_b, thread_b = _stub_state_writer(output_manager, port_b)

        output_manager.close_port_storage_writers()

        writer_a.stop.assert_called_once()
        writer_b.stop.assert_called_once()
        thread_a.join.assert_called_once()
        thread_b.join.assert_called_once()
        assert output_manager._port_state_writers == {}

    def test_defaults_loop_columns_when_omitted(self, output_manager, state, port_a):
        # Dormancy: callers that pass no loop bookkeeping (every non-loop
        # caller, e.g. MainLoop.process_input_state) still produce a valid
        # 3-column state tuple with the loop columns at their no-loop defaults.
        queue_a, _, _ = _stub_state_writer(output_manager, port_a)

        output_manager.save_state_to_storage_if_needed(state)  # no loop_counter

        data_tuple = queue_a.put.call_args.args[0].data_tuple
        assert data_tuple[State.LOOP_COUNTER] == 0
        assert data_tuple[State.LOOP_START_ID] == ""


class TestResetOutputStorage:
    """Covers OutputManager.reset_output_storage, the per-iteration
    result+state table reset a Loop End worker runs between loop
    iterations.

    The collaborators that touch real iceberg storage / writer threads
    (DocumentFactory, close_port_storage_writers,
    set_up_port_storage_writer) are replaced with spies so these tests
    stay hermetic and assert the contract: drop+recreate both tables,
    bracketed by closing the old writers and opening fresh ones, with
    both preconditions enforced.
    """

    @pytest.fixture
    def output_manager(self):
        return OutputManager(worker_id="Worker:WF0-test-op-main-0")

    @staticmethod
    def _add_port_with_storage(om, port_id, uri, schema):
        # Stand in for what add_output_port + set_up_port_storage_writer
        # populate, without spinning up real iceberg tables and threads.
        port = MagicMock()
        port.get_schema.return_value = schema
        om._ports[port_id] = port
        om._storage_uri_base = uri

    def test_recreates_result_and_state_tables_and_reopens_writer(self, output_manager):
        port_id = PortIdentity(id=0, internal=False)
        schema = MagicMock(name="schema")
        self._add_port_with_storage(output_manager, port_id, "vfs:///base", schema)

        output_manager.close_port_storage_writers = MagicMock()
        output_manager.set_up_port_storage_writer = MagicMock()

        with (
            patch(
                "core.architecture.packaging.output_manager.DocumentFactory"
            ) as doc_factory,
            patch(
                "core.architecture.packaging.output_manager.VFSURIFactory"
            ) as uri_factory,
        ):
            uri_factory.result_uri.return_value = "vfs:///base/result"
            uri_factory.state_uri.return_value = "vfs:///base/state"
            output_manager.reset_output_storage()

        # Both the result and the state table are recreated, which drops
        # the rows the previous loop iteration wrote. The result table must
        # get the port's schema and the state table the State schema.
        recreated = {
            call.args[0]: call.args[1]
            for call in doc_factory.create_document.call_args_list
        }
        assert recreated == {
            "vfs:///base/result": schema,
            "vfs:///base/state": State.SCHEMA,
        }
        # The old writers are flushed/closed first, and fresh writers are
        # opened against the recreated tables afterwards.
        output_manager.close_port_storage_writers.assert_called_once_with()
        output_manager.set_up_port_storage_writer.assert_called_once_with(
            port_id, "vfs:///base"
        )

    def test_raises_when_no_output_port(self, output_manager):
        output_manager._storage_uri_base = "vfs:///base"
        output_manager.close_port_storage_writers = MagicMock()
        with patch("core.architecture.packaging.output_manager.DocumentFactory"):
            with pytest.raises(RuntimeError, match="exactly one output port"):
                output_manager.reset_output_storage()
        # Must fail before touching storage.
        output_manager.close_port_storage_writers.assert_not_called()

    def test_raises_when_multiple_output_ports(self, output_manager):
        schema = MagicMock()
        self._add_port_with_storage(
            output_manager, PortIdentity(id=0, internal=False), "vfs:///base", schema
        )
        # A second port makes the count != 1; the shared _storage_uri_base
        # is already set, so the port-count guard is what must trip.
        output_manager._ports[PortIdentity(id=1, internal=False)] = MagicMock()
        with pytest.raises(RuntimeError, match="exactly one output port"):
            output_manager.reset_output_storage()

    def test_raises_when_storage_writer_not_set_up(self, output_manager):
        # The port exists but no storage URI was assigned -- i.e.
        # set_up_port_storage_writer never ran for it.
        output_manager._ports[PortIdentity(id=0, internal=False)] = MagicMock()
        output_manager.close_port_storage_writers = MagicMock()
        with patch("core.architecture.packaging.output_manager.DocumentFactory"):
            with pytest.raises(RuntimeError, match="storage writer was set up"):
                output_manager.reset_output_storage()
        output_manager.close_port_storage_writers.assert_not_called()


class TestSaveTupleToStorageIfNeeded:
    """Mirrors TestSaveStateToStorageIfNeeded for the tuple-side twin,
    save_tuple_to_storage_if_needed, which routes result tuples (rather
    than state rows) to the per-port writer queues."""

    @pytest.fixture
    def output_manager(self):
        return OutputManager(worker_id=_WORKER_ID)

    @pytest.fixture
    def port_a(self):
        return PortIdentity(id=0, internal=False)

    @pytest.fixture
    def port_b(self):
        return PortIdentity(id=1, internal=False)

    @pytest.fixture
    def tuple_(self):
        return Tuple({"k": 1, "v": "a"})

    def test_no_tuple_writers_is_a_noop(self, output_manager, tuple_):
        # With no port set up, save_tuple_to_storage_if_needed must not
        # touch any writer.
        output_manager.save_tuple_to_storage_if_needed(tuple_)  # no-op

    def test_unknown_port_id_is_a_noop(self, output_manager, tuple_, port_a, port_b):
        queue_a, _, _ = _stub_tuple_writer(output_manager, port_a)
        output_manager.save_tuple_to_storage_if_needed(tuple_, port_id=port_b)
        # A port the scheduler never marked for storage must not receive
        # the tuple, and no other port may receive it either.
        queue_a.put.assert_not_called()

    def test_enqueues_to_every_port_when_port_id_omitted(
        self, output_manager, tuple_, port_a, port_b
    ):
        queue_a, _, _ = _stub_tuple_writer(output_manager, port_a)
        queue_b, _, _ = _stub_tuple_writer(output_manager, port_b)

        output_manager.save_tuple_to_storage_if_needed(tuple_)

        # Each port's writer queue receives one PortStorageWriterElement;
        # the save is non-blocking, so nothing may touch the buffered
        # writer directly (that happens off-thread).
        assert queue_a.put.call_count == 1
        assert queue_b.put.call_count == 1
        assert isinstance(queue_a.put.call_args.args[0], PortStorageWriterElement)
        assert isinstance(queue_b.put.call_args.args[0], PortStorageWriterElement)

    def test_enqueues_only_to_selected_port_when_port_id_specified(
        self, output_manager, tuple_, port_a, port_b
    ):
        queue_a, _, _ = _stub_tuple_writer(output_manager, port_a)
        queue_b, _, _ = _stub_tuple_writer(output_manager, port_b)

        output_manager.save_tuple_to_storage_if_needed(tuple_, port_id=port_a)

        assert queue_a.put.call_count == 1
        queue_b.put.assert_not_called()

    def test_wraps_the_exact_tuple_in_the_writer_element(
        self, output_manager, tuple_, port_a
    ):
        # Unlike the state twin (which converts State to a 3-column tuple
        # with loop bookkeeping), the tuple path must forward the produced
        # tuple as-is, without copying or transformation.
        queue_a, _, _ = _stub_tuple_writer(output_manager, port_a)

        output_manager.save_tuple_to_storage_if_needed(tuple_, port_id=port_a)

        element = queue_a.put.call_args.args[0]
        assert isinstance(element, PortStorageWriterElement)
        assert element.data_tuple is tuple_

    def test_close_port_storage_writers_stops_tuple_threads(
        self, output_manager, port_a, port_b
    ):
        # close_port_storage_writers must cover the tuple-writer registry,
        # not just the state-writer one asserted elsewhere.
        _, writer_a, thread_a = _stub_tuple_writer(output_manager, port_a)
        _, writer_b, thread_b = _stub_tuple_writer(output_manager, port_b)

        output_manager.close_port_storage_writers()

        writer_a.stop.assert_called_once()
        writer_b.stop.assert_called_once()
        thread_a.join.assert_called_once()
        thread_b.join.assert_called_once()
        assert output_manager._port_storage_writers == {}


class TestAddOutputPort:
    @pytest.fixture
    def output_manager(self):
        return OutputManager(worker_id=_WORKER_ID)

    def test_normalizes_none_id_to_zero(self, output_manager):
        # Scala-side messages may leave the default port id unset; the
        # port must land under the canonical PortIdentity(id=0).
        port_id = PortIdentity(internal=False)
        port_id.id = None

        output_manager.add_output_port(port_id, MagicMock(name="schema"))

        assert port_id.id == 0
        assert output_manager.get_port_ids() == [PortIdentity(id=0, internal=False)]

    def test_normalizes_none_internal_to_false(self, output_manager):
        port_id = PortIdentity(id=0)
        port_id.internal = None

        output_manager.add_output_port(port_id, MagicMock(name="schema"))

        assert port_id.internal is False
        assert output_manager.get_port_ids() == [PortIdentity(id=0, internal=False)]

    def test_port_can_only_be_added_once(self, output_manager):
        # A second add of the same port id must not replace the port (or
        # its schema) registered by the first add.
        schema_first = MagicMock(name="schema_first")
        port_id = PortIdentity(id=0, internal=False)

        output_manager.add_output_port(port_id, schema_first)
        output_manager.add_output_port(
            PortIdentity(id=0, internal=False), MagicMock(name="schema_second")
        )

        assert output_manager.get_port_ids() == [port_id]
        assert output_manager.get_port().get_schema() is schema_first

    def test_sets_up_storage_writer_only_when_uri_given(self, output_manager):
        output_manager.set_up_port_storage_writer = MagicMock()
        port_a = PortIdentity(id=0, internal=False)
        port_b = PortIdentity(id=1, internal=False)

        output_manager.add_output_port(port_a, MagicMock())
        output_manager.set_up_port_storage_writer.assert_not_called()

        output_manager.add_output_port(
            port_b, MagicMock(), storage_uri_base="vfs:///base"
        )
        output_manager.set_up_port_storage_writer.assert_called_once_with(
            port_b, "vfs:///base"
        )


class TestAddPartitioning:
    """Drives every entry of the Partitioning -> Partitioner dispatch
    table through OutputManager.add_partitioning (rather than
    constructing partitioners directly as test_partitioners.py does)."""

    @pytest.fixture
    def output_manager(self):
        return OutputManager(worker_id=_WORKER_ID)

    @pytest.fixture
    def link(self):
        return PhysicalLink()

    @pytest.mark.parametrize(
        "partitioning,expected_partitioner",
        [
            (
                OneToOnePartitioning(batch_size=1, channels=[_self_channel("A")]),
                OneToOnePartitioner,
            ),
            (
                RoundRobinPartitioning(batch_size=1, channels=[_self_channel("A")]),
                RoundRobinPartitioner,
            ),
            (
                HashBasedShufflePartitioning(
                    batch_size=1,
                    channels=[_self_channel("A")],
                    hash_attribute_names=["k"],
                ),
                HashBasedShufflePartitioner,
            ),
            (
                RangeBasedShufflePartitioning(
                    batch_size=1,
                    channels=[_self_channel("A")],
                    range_attribute_names=["k"],
                    range_min=0,
                    range_max=9,
                ),
                RangeBasedShufflePartitioner,
            ),
            (
                BroadcastPartitioning(batch_size=1, channels=[_self_channel("A")]),
                BroadcastPartitioner,
            ),
        ],
        ids=["one_to_one", "round_robin", "hash", "range", "broadcast"],
    )
    def test_dispatches_each_partitioning_to_its_partitioner(
        self, output_manager, link, partitioning, expected_partitioner
    ):
        output_manager.add_partitioning(link, set_one_of(Partitioning, partitioning))
        assert type(output_manager._partitioners[link]) is expected_partitioner

    def test_registers_only_channels_sent_from_this_worker(self, output_manager, link):
        mine = _self_channel("A")
        someone_elses = ChannelIdentity(
            from_worker_id=_worker("Worker:WF0-other-main-0"),
            to_worker_id=_worker("B"),
        )

        output_manager.add_partitioning(
            link,
            set_one_of(
                Partitioning,
                BroadcastPartitioning(batch_size=1, channels=[mine, someone_elses]),
            ),
        )

        assert list(output_manager.get_output_channel_ids()) == [mine]

    def test_canonicalizes_is_control_before_registering(self, output_manager, link):
        # add_partitioning pins is_control eagerly so the registered
        # ChannelIdentity hashes consistently.
        mine = _self_channel("A")
        mine.is_control = None

        output_manager.add_partitioning(
            link,
            set_one_of(
                Partitioning, OneToOnePartitioning(batch_size=1, channels=[mine])
            ),
        )

        assert mine.is_control is False
        assert mine in output_manager._channels

    def test_one_to_one_selects_receiver_by_this_worker_id(self, output_manager, link):
        # OneToOnePartitioner is the only entry that needs the worker id:
        # it must pick the channel whose sender is this worker.
        partitioning = OneToOnePartitioning(
            batch_size=1,
            channels=[
                ChannelIdentity(
                    from_worker_id=_worker("Worker:WF0-other-main-0"),
                    to_worker_id=_worker("X"),
                ),
                _self_channel("A"),
            ],
        )

        output_manager.add_partitioning(link, set_one_of(Partitioning, partitioning))

        assert output_manager._partitioners[link].receiver == _worker("A")


_EMIT_SCHEMA = Schema(raw_schema={"k": "INTEGER", "v": "STRING"})


class TestEmitChain:
    """Covers the emit path: partitioner fan-out in tuple_to_batch /
    emit_ecm / emit_state, and serialization against the output port's
    schema in tuple_to_frame."""

    @pytest.fixture
    def output_manager(self):
        om = OutputManager(worker_id=_WORKER_ID)
        om.add_output_port(PortIdentity(id=0, internal=False), _EMIT_SCHEMA)
        return om

    @pytest.fixture
    def link(self):
        return PhysicalLink()

    @staticmethod
    def _add_one_to_one(om, link, receiver_name, batch_size):
        om.add_partitioning(
            link,
            set_one_of(
                Partitioning,
                OneToOnePartitioning(
                    batch_size=batch_size,
                    channels=[_self_channel(receiver_name)],
                ),
            ),
        )

    def test_tuple_to_batch_below_batch_size_emits_nothing(self, output_manager, link):
        self._add_one_to_one(output_manager, link, "A", batch_size=2)
        assert list(output_manager.tuple_to_batch(Tuple({"k": 1, "v": "a"}))) == []

    def test_tuple_to_batch_emits_frame_to_receiver_at_batch_size(
        self, output_manager, link
    ):
        self._add_one_to_one(output_manager, link, "A", batch_size=2)
        list(output_manager.tuple_to_batch(Tuple({"k": 1, "v": "a"})))

        out = list(output_manager.tuple_to_batch(Tuple({"k": 2, "v": "b"})))

        assert len(out) == 1
        receiver, frame = out[0]
        assert receiver == _worker("A")
        assert isinstance(frame, DataFrame)
        assert frame.frame.to_pydict() == {"k": [1, 2], "v": ["a", "b"]}

    def test_tuple_to_batch_fans_out_to_every_partitioner(self, output_manager):
        # Two downstream links: the same tuple must reach both, in the
        # order the partitionings were added.
        link_a = PhysicalLink(to_port_id=PortIdentity(id=1))
        link_b = PhysicalLink(to_port_id=PortIdentity(id=2))
        self._add_one_to_one(output_manager, link_a, "A", batch_size=1)
        self._add_one_to_one(output_manager, link_b, "B", batch_size=1)

        out = list(output_manager.tuple_to_batch(Tuple({"k": 1, "v": "a"})))

        assert [receiver for receiver, _ in out] == [_worker("A"), _worker("B")]
        for _, frame in out:
            assert frame.frame.to_pydict() == {"k": [1], "v": ["a"]}

    def test_tuple_to_frame_serializes_against_port_schema(self, output_manager):
        frame = output_manager.tuple_to_frame(
            [Tuple({"k": 1, "v": "a"}), Tuple({"k": 2, "v": "b"})]
        )

        assert isinstance(frame, DataFrame)
        assert frame.frame.schema == _EMIT_SCHEMA.as_arrow_schema()
        assert frame.frame.to_pydict() == {"k": [1, 2], "v": ["a", "b"]}

    def test_emit_ecm_flushes_pending_batch_then_passes_ecm_through(
        self, output_manager, link
    ):
        self._add_one_to_one(output_manager, link, "A", batch_size=2)
        list(output_manager.tuple_to_batch(Tuple({"k": 1, "v": "a"})))
        ecm = EmbeddedControlMessage()

        out = list(output_manager.emit_ecm(_worker("A"), ecm))

        assert len(out) == 2
        # The pending half-full batch is flushed as a serialized frame...
        assert isinstance(out[0], DataFrame)
        assert out[0].frame.to_pydict() == {"k": [1], "v": ["a"]}
        # ...while the ECM itself passes through unserialized.
        assert out[1] is ecm

    def test_emit_ecm_with_no_pending_batch_yields_only_ecm(self, output_manager, link):
        self._add_one_to_one(output_manager, link, "A", batch_size=2)
        ecm = EmbeddedControlMessage()

        assert list(output_manager.emit_ecm(_worker("A"), ecm)) == [ecm]

    def test_emit_state_flushes_pending_batch_then_wraps_state_frame(
        self, output_manager, link
    ):
        self._add_one_to_one(output_manager, link, "A", batch_size=2)
        list(output_manager.tuple_to_batch(Tuple({"k": 1, "v": "a"})))
        state = State({"i": 2})

        out = list(
            output_manager.emit_state(state, loop_counter=3, loop_start_id="loop-0")
        )

        assert len(out) == 2
        receiver, frame = out[0]
        assert receiver == _worker("A")
        assert isinstance(frame, DataFrame)
        assert frame.frame.to_pydict() == {"k": [1], "v": ["a"]}
        receiver, state_frame = out[1]
        assert receiver == _worker("A")
        assert isinstance(state_frame, StateFrame)
        assert state_frame.frame is state
        assert state_frame.loop_counter == 3
        assert state_frame.loop_start_id == "loop-0"

    def test_emit_state_defaults_loop_columns_when_omitted(self, output_manager, link):
        self._add_one_to_one(output_manager, link, "A", batch_size=2)

        out = list(output_manager.emit_state(State({"i": 2})))

        ((receiver, state_frame),) = out
        assert receiver == _worker("A")
        assert state_frame.loop_counter == 0
        assert state_frame.loop_start_id == ""


class TestQueryMethods:
    @pytest.fixture
    def output_manager(self):
        return OutputManager(worker_id=_WORKER_ID)

    def test_is_missing_output_ports_flips_once_a_port_is_added(self, output_manager):
        # The region scheduler relies on this to detect the dependee-only
        # execution phase, where a worker legitimately has no output port.
        assert output_manager.is_missing_output_ports()
        output_manager.add_output_port(PortIdentity(id=0, internal=False), MagicMock())
        assert not output_manager.is_missing_output_ports()

    def test_get_port_ids_returns_added_ports_in_insertion_order(self, output_manager):
        port_a = PortIdentity(id=0, internal=False)
        port_b = PortIdentity(id=1, internal=False)
        output_manager.add_output_port(port_a, MagicMock())
        output_manager.add_output_port(port_b, MagicMock())
        assert output_manager.get_port_ids() == [port_a, port_b]

    def test_get_output_channel_ids_lists_channels_from_add_partitioning(
        self, output_manager
    ):
        assert list(output_manager.get_output_channel_ids()) == []
        output_manager.add_partitioning(
            PhysicalLink(),
            set_one_of(
                Partitioning,
                OneToOnePartitioning(batch_size=1, channels=[_self_channel("A")]),
            ),
        )
        assert list(output_manager.get_output_channel_ids()) == [
            ChannelIdentity(
                from_worker_id=_worker(_WORKER_ID),
                to_worker_id=_worker("A"),
                is_control=False,
            )
        ]
