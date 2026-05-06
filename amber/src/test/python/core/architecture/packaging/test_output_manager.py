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
from proto.org.apache.texera.amber.core import PortIdentity


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
        return State({"loop_counter": 1, "i": 2})

    def test_no_state_writers_is_a_noop(self, output_manager, state):
        # With no port set up, save_state_to_storage_if_needed must not
        # touch any writer.
        output_manager.save_state_to_storage_if_needed(state)  # no-op, no exception

    def test_unknown_port_id_is_a_noop(self, output_manager, state, port_a):
        output_manager.save_state_to_storage_if_needed(state, port_id=port_a)
        # No assertion needed -- the absence of any writer means nothing
        # was attempted.

    def test_writes_to_every_port_when_port_id_omitted(
        self, output_manager, state, port_a, port_b
    ):
        writer_a = MagicMock()
        writer_b = MagicMock()
        output_manager._state_writers[port_a] = writer_a
        output_manager._state_writers[port_b] = writer_b

        output_manager.save_state_to_storage_if_needed(state)

        writer_a.put_one.assert_called_once()
        writer_b.put_one.assert_called_once()
        # Long-lived writers must NOT be closed per state -- otherwise
        # we'd be back to one Iceberg snapshot per state.
        writer_a.close.assert_not_called()
        writer_b.close.assert_not_called()

    def test_writes_only_to_selected_port_when_port_id_specified(
        self, output_manager, state, port_a, port_b
    ):
        writer_a = MagicMock()
        writer_b = MagicMock()
        output_manager._state_writers[port_a] = writer_a
        output_manager._state_writers[port_b] = writer_b

        output_manager.save_state_to_storage_if_needed(state, port_id=port_a)

        writer_a.put_one.assert_called_once()
        writer_b.put_one.assert_not_called()

    def test_state_writer_is_opened_at_port_setup(self, output_manager, port_a):
        # set_up_port_storage_writer should open the result document AND
        # the state document, then cache the state writer for reuse.
        result_doc = MagicMock()
        state_doc = MagicMock()
        state_writer = MagicMock()
        state_doc.writer.return_value = state_writer

        with patch(
            "core.architecture.packaging.output_manager.DocumentFactory"
        ) as mock_factory:
            mock_factory.open_document.side_effect = [
                (result_doc, MagicMock()),
                (state_doc, MagicMock()),
            ]

            output_manager.set_up_port_storage_writer(
                port_a, "vfs:///wf/0/exec/0/result/op-a"
            )

            opened = [c.args[0] for c in mock_factory.open_document.call_args_list]
            assert opened == [
                "vfs:///wf/0/exec/0/result/op-a",
                "vfs:///wf/0/exec/0/state/op-a",
            ]
            state_writer.open.assert_called_once()
            assert output_manager._state_writers[port_a] is state_writer

    def test_close_port_storage_writers_flushes_state_writers(
        self, output_manager, port_a, port_b
    ):
        # After the port completes, the long-lived state writer's buffer
        # must be flushed and the writer closed (one Iceberg commit per
        # port instead of one per state).
        writer_a = MagicMock()
        writer_b = MagicMock()
        output_manager._state_writers[port_a] = writer_a
        output_manager._state_writers[port_b] = writer_b

        output_manager.close_port_storage_writers()

        writer_a.close.assert_called_once()
        writer_b.close.assert_called_once()
        assert output_manager._state_writers == {}
