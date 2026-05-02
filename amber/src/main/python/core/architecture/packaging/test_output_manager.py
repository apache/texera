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

    def _stub_document_factory(self, mock_factory):
        document = MagicMock()
        writer = MagicMock()
        document.writer.return_value = writer
        mock_factory.open_document.return_value = (document, MagicMock())
        return document, writer

    def test_no_storage_uris_is_a_noop(self, output_manager, state):
        # save_state_to_storage_if_needed must not touch DocumentFactory when
        # the worker has no provisioned output storage.
        with patch(
            "core.architecture.packaging.output_manager.DocumentFactory"
        ) as mock_factory:
            output_manager.save_state_to_storage_if_needed(state)
            mock_factory.open_document.assert_not_called()
            mock_factory.create_document.assert_not_called()

    def test_unknown_port_id_is_a_noop(self, output_manager, state, port_a):
        with patch(
            "core.architecture.packaging.output_manager.DocumentFactory"
        ) as mock_factory:
            output_manager.save_state_to_storage_if_needed(state, port_id=port_a)
            mock_factory.open_document.assert_not_called()

    def test_writes_to_every_port_when_port_id_omitted(
        self, output_manager, state, port_a, port_b
    ):
        output_manager._storage_uris[port_a] = "vfs:///wf/0/exec/0/result/op-a"
        output_manager._storage_uris[port_b] = "vfs:///wf/0/exec/0/result/op-b"

        with patch(
            "core.architecture.packaging.output_manager.DocumentFactory"
        ) as mock_factory:
            _, writer = self._stub_document_factory(mock_factory)

            output_manager.save_state_to_storage_if_needed(state)

            assert mock_factory.open_document.call_count == 2
            opened_uris = {
                call.args[0] for call in mock_factory.open_document.call_args_list
            }
            assert opened_uris == {
                "vfs:///wf/0/exec/0/state/op-a",
                "vfs:///wf/0/exec/0/state/op-b",
            }
            assert writer.put_one.call_count == 2
            assert writer.close.call_count == 2

    def test_writes_only_to_selected_port_when_port_id_specified(
        self, output_manager, state, port_a, port_b
    ):
        output_manager._storage_uris[port_a] = "vfs:///wf/0/exec/0/result/op-a"
        output_manager._storage_uris[port_b] = "vfs:///wf/0/exec/0/result/op-b"

        with patch(
            "core.architecture.packaging.output_manager.DocumentFactory"
        ) as mock_factory:
            self._stub_document_factory(mock_factory)

            output_manager.save_state_to_storage_if_needed(state, port_id=port_a)

            assert mock_factory.open_document.call_count == 1
            assert (
                mock_factory.open_document.call_args.args[0]
                == "vfs:///wf/0/exec/0/state/op-a"
            )

    def test_creates_document_when_open_raises_value_error(
        self, output_manager, state, port_a
    ):
        # The first time a state is saved, the state document does not yet
        # exist; open_document raises ValueError and we must fall back to
        # create_document so the state still gets written.
        output_manager._storage_uris[port_a] = "vfs:///wf/0/exec/0/result/op-a"

        with patch(
            "core.architecture.packaging.output_manager.DocumentFactory"
        ) as mock_factory:
            mock_factory.open_document.side_effect = ValueError("not found")
            created_document = MagicMock()
            writer = MagicMock()
            created_document.writer.return_value = writer
            mock_factory.create_document.return_value = created_document

            output_manager.save_state_to_storage_if_needed(state)

            mock_factory.create_document.assert_called_once_with(
                "vfs:///wf/0/exec/0/state/op-a", State.SCHEMA
            )
            writer.put_one.assert_called_once()
            writer.close.assert_called_once()

    def test_uri_is_recorded_when_storage_writer_is_set_up(
        self, output_manager, port_a
    ):
        # set_up_port_storage_writer should populate _storage_uris so that a
        # subsequent save_state_to_storage_if_needed can find the URI.
        with patch(
            "core.architecture.packaging.output_manager.DocumentFactory"
        ) as mock_factory:
            mock_factory.open_document.return_value = (MagicMock(), MagicMock())

            output_manager.set_up_port_storage_writer(
                port_a, "vfs:///wf/0/exec/0/result/op-a"
            )

            assert (
                output_manager._storage_uris[port_a]
                == "vfs:///wf/0/exec/0/result/op-a"
            )
