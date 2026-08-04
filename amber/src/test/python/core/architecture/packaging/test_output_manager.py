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
from core.models.state import State
from core.storage.runnables.port_storage_writer import PortStorageWriterElement
from proto.org.apache.texera.amber.core import PortIdentity


def _stub_state_writer(output_manager, port_id):
    """Inject a (queue, writer, thread) triple as if a port were set up."""
    queue = MagicMock()
    writer = MagicMock()
    thread = MagicMock()
    output_manager._port_state_writers[port_id] = (queue, writer, thread)
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
        # the rows the previous loop iteration wrote.
        recreated = {
            call.args[0] for call in doc_factory.create_document.call_args_list
        }
        assert recreated == {"vfs:///base/result", "vfs:///base/state"}
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
