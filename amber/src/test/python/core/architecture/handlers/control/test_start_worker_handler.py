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

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from core.architecture.handlers.control.start_worker_handler import StartWorkerHandler
from core.architecture.managers.context import WORKER_STATE_TRANSITIONS
from core.architecture.managers.state_manager import (
    InvalidTransitionException,
    StateManager,
)
from core.architecture.packaging.input_manager import InputManager
from core.models.internal_queue import ECMElement, InternalQueue
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    EmbeddedControlMessageIdentity,
    PortIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmbeddedControlMessageType,
    EmptyRequest,
    WorkerStateResponse,
)
from proto.org.apache.texera.amber.engine.architecture.worker import WorkerState

WORKER_ID = "worker-1"

SOURCE_CHANNEL = ChannelIdentity(
    InputManager.SOURCE_STARTER, ActorVirtualIdentity(WORKER_ID), False
)
SOURCE_PORT = PortIdentity(0, False)


def make_context(
    is_source: bool,
    initial_state: WorkerState = WorkerState.READY,
    input_manager=None,
):
    """Wire the slice of Context that StartWorkerHandler touches.

    The state manager and the input queue are the real ones so state
    transitions and the self-fed source markers are asserted against real
    behavior; only the executor is stubbed, since the handler reads nothing
    from it but `is_source`.
    """
    input_queue = InternalQueue()
    return SimpleNamespace(
        worker_id=WORKER_ID,
        input_queue=input_queue,
        executor_manager=SimpleNamespace(executor=SimpleNamespace(is_source=is_source)),
        input_manager=(
            input_manager
            if input_manager is not None
            else InputManager(WORKER_ID, input_queue)
        ),
        state_manager=StateManager(WORKER_STATE_TRANSITIONS, initial_state),
        current_input_channel_id=None,
    )


def start(context) -> WorkerStateResponse:
    return asyncio.run(StartWorkerHandler(context).start_worker(EmptyRequest()))


def drain(input_queue: InternalQueue):
    return [input_queue.get() for _ in range(input_queue.size())]


class TestStartWorkerOnSource:
    def test_transits_to_running_and_reports_the_bumped_version(self):
        context = make_context(is_source=True)

        response = start(context)

        assert isinstance(response, WorkerStateResponse)
        assert response.state == WorkerState.RUNNING
        # A source worker really transitions here, so the state manager's
        # logical clock must advance and be reported with the new state.
        assert response.state_version == 1
        assert context.state_manager.get_current_state() == WorkerState.RUNNING

    def test_registers_the_source_starter_channel_on_port_zero(self):
        context = make_context(is_source=True)

        start(context)

        # A source has no upstream, so the handler fabricates port 0 and a
        # channel from the synthetic SOURCE_STARTER actor to itself.
        assert context.current_input_channel_id == SOURCE_CHANNEL
        assert context.input_manager.get_port_id(SOURCE_CHANNEL) == SOURCE_PORT
        port = context.input_manager.get_port(SOURCE_PORT)
        assert port.get_channels() == {SOURCE_CHANNEL}
        # The synthetic port carries no attributes and no materialized readers.
        assert port.get_schema().get_attr_names() == []
        assert context.input_manager.get_input_port_mat_reader_threads() == {
            SOURCE_PORT: []
        }

    def test_self_feeds_start_then_end_channel_markers(self):
        context = make_context(is_source=True)

        start(context)

        elements = drain(context.input_queue)
        assert len(elements) == 2
        assert all(isinstance(element, ECMElement) for element in elements)
        assert [element.tag for element in elements] == [SOURCE_CHANNEL] * 2

        start_ecm, end_ecm = (element.payload for element in elements)
        # The start marker opens the channel without alignment; the end marker
        # must be port-aligned so downstream completion is ordered correctly.
        assert start_ecm.id == EmbeddedControlMessageIdentity("StartChannel")
        assert start_ecm.ecm_type == EmbeddedControlMessageType.NO_ALIGNMENT
        assert end_ecm.id == EmbeddedControlMessageIdentity("EndChannel")
        assert end_ecm.ecm_type == EmbeddedControlMessageType.PORT_ALIGNMENT

    def test_markers_carry_a_command_addressed_to_this_worker(self):
        context = make_context(is_source=True)

        start(context)

        start_ecm, end_ecm = (element.payload for element in drain(context.input_queue))
        for ecm, method in ((start_ecm, "StartChannel"), (end_ecm, "EndChannel")):
            assert ecm.scope == []
            # The mapping is keyed by the receiving worker, which is this
            # worker itself since the source feeds its own input queue.
            assert list(ecm.command_mapping) == [WORKER_ID]
            invocation = ecm.command_mapping[WORKER_ID]
            assert invocation.method_name == method
            assert invocation.command_id == -1

    def test_a_started_source_is_left_running_on_a_second_start(self):
        # transit_to() is a no-op when already in the target state, so a
        # repeated start must not raise nor bump the reported version.
        context = make_context(is_source=True)
        start(context)

        response = start(context)

        assert response.state == WorkerState.RUNNING
        assert response.state_version == 1

    def test_rejects_a_start_before_the_worker_is_ready(self):
        # UNINITIALIZED -> RUNNING is not a legal edge; the handler must fail
        # loudly instead of half-starting the source.
        context = make_context(is_source=True, initial_state=WorkerState.UNINITIALIZED)

        with pytest.raises(InvalidTransitionException):
            start(context)

        assert context.state_manager.get_current_state() == WorkerState.UNINITIALIZED
        assert context.input_queue.size() == 0
        assert context.current_input_channel_id is None


class TestStartWorkerOnNonSource:
    def test_starts_materialization_reader_threads_when_present(self):
        input_manager = MagicMock(spec=InputManager)
        input_manager.get_input_port_mat_reader_threads.return_value = {
            SOURCE_PORT: [object()]
        }
        context = make_context(is_source=False, input_manager=input_manager)

        response = start(context)

        input_manager.start_input_port_mat_reader_threads.assert_called_once_with()
        # Reading from a materialized port does not start the worker; the
        # main loop transitions on the first arriving tuple instead.
        assert response.state == WorkerState.READY
        assert response.state_version == 0
        assert context.input_queue.size() == 0
        assert context.current_input_channel_id is None

    def test_does_nothing_without_materialization_reader_threads(self):
        input_manager = MagicMock(spec=InputManager)
        input_manager.get_input_port_mat_reader_threads.return_value = {}
        context = make_context(is_source=False, input_manager=input_manager)

        response = start(context)

        input_manager.start_input_port_mat_reader_threads.assert_not_called()
        input_manager.add_input_port.assert_not_called()
        input_manager.register_input.assert_not_called()
        assert response.state == WorkerState.READY
        assert response.state_version == 0
        assert context.input_queue.size() == 0

    def test_reports_a_paused_state_untouched(self):
        # The handler is a pure reporter for a non-source worker: whatever
        # state the worker is in is echoed back with its version.
        input_manager = MagicMock(spec=InputManager)
        input_manager.get_input_port_mat_reader_threads.return_value = {}
        context = make_context(is_source=False, input_manager=input_manager)
        context.state_manager.transit_to(WorkerState.PAUSED)

        response = start(context)

        assert response.state == WorkerState.PAUSED
        assert response.state_version == 1
