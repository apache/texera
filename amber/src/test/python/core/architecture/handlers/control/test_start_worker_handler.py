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
from typing import Optional
from unittest.mock import MagicMock

import pytest

from core.architecture.handlers.control.start_worker_handler import StartWorkerHandler
from core.architecture.managers.context import WORKER_STATE_TRANSITIONS
from core.architecture.managers.state_manager import (
    InvalidTransitionException,
    StateManager,
)
from core.architecture.packaging.input_manager import InputManager
from core.models import Schema
from core.models.internal_queue import InternalQueue
from core.util.proto import get_one_of
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    ChannelIdentity,
    PortIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmbeddedControlMessageType,
    EmptyRequest,
    WorkerStateResponse,
)
from proto.org.apache.texera.amber.engine.architecture.worker import WorkerState

WORKER_ID = "worker-1"

# The synthetic channel a source worker starts itself over: it has no upstream,
# so the handler fabricates a SOURCE_STARTER -> self channel to carry the
# start/end markers.
SOURCE_STARTER_CHANNEL = ChannelIdentity(
    InputManager.SOURCE_STARTER, ActorVirtualIdentity(WORKER_ID), False
)

# The no-reply marker: these invocations are fire-and-forget, so the controller
# must not be left awaiting a promise for them.
NO_REPLY_COMMAND_ID = -1

# A non-default version so a regression that drops state_version — or hardcodes
# it to 0 — cannot pass.
STATE_VERSION = 17


def _build_handler(
    is_source: bool, mat_reader_threads: Optional[dict] = None
) -> StartWorkerHandler:
    """Wire a handler with a SimpleNamespace context exposing everything
    start_worker touches, a real InternalQueue so enqueue ordering is observed
    through the production queue, and mocked managers."""
    state_manager = MagicMock()
    state_manager.get_state_with_version.return_value = (
        WorkerState.RUNNING,
        STATE_VERSION,
    )
    input_manager = MagicMock()
    input_manager.get_input_port_mat_reader_threads.return_value = (
        {} if mat_reader_threads is None else mat_reader_threads
    )
    context = SimpleNamespace(
        executor_manager=SimpleNamespace(executor=SimpleNamespace(is_source=is_source)),
        state_manager=state_manager,
        input_manager=input_manager,
        input_queue=InternalQueue(),
        worker_id=WORKER_ID,
        current_input_channel_id=None,
    )
    return StartWorkerHandler(context)


def _build_real_handler(
    is_source: bool, initial_state: WorkerState, input_manager=None
) -> StartWorkerHandler:
    """Wire a handler against the real StateManager and InputManager rather than
    mocks, so the collaborators' own invariants take part in the test. The
    InputManager and the context share one InternalQueue, exactly as Context
    constructs them. The non-source branches pass an InputManager double, since
    there is no public way to make a real one report reader threads."""
    queue = InternalQueue()
    context = SimpleNamespace(
        executor_manager=SimpleNamespace(executor=SimpleNamespace(is_source=is_source)),
        state_manager=StateManager(WORKER_STATE_TRANSITIONS, initial_state),
        input_manager=(
            InputManager(WORKER_ID, queue) if input_manager is None else input_manager
        ),
        input_queue=queue,
        worker_id=WORKER_ID,
        current_input_channel_id=None,
    )
    return StartWorkerHandler(context)


def _drain(queue: InternalQueue) -> list:
    """Pop everything currently queued, preserving enqueue order."""
    return [queue.get() for _ in range(queue.size())]


def _summarize(element) -> tuple:
    """Reduce an ECMElement to the fields the start protocol is defined by."""
    (command,) = element.payload.command_mapping.values()
    return (
        element.tag,
        element.payload.id.id,
        element.payload.ecm_type,
        set(element.payload.command_mapping),
        command.method_name,
        command.command_id,
    )


class TestStartWorkerHandler:
    @pytest.fixture
    def source_handler(self):
        return _build_handler(is_source=True)

    @pytest.fixture
    def mat_reader_handler(self):
        return _build_handler(
            is_source=False,
            mat_reader_threads={PortIdentity(0, False): [MagicMock()]},
        )

    @pytest.fixture
    def fall_through_handler(self):
        return _build_handler(is_source=False)

    # --- source branch ---------------------------------------------------

    def test_source_transits_to_running(self, source_handler):
        asyncio.run(source_handler.start_worker(EmptyRequest()))
        source_handler.context.state_manager.transit_to.assert_called_once_with(
            WorkerState.RUNNING
        )

    def test_source_adds_the_synthetic_input_port(self, source_handler):
        asyncio.run(source_handler.start_worker(EmptyRequest()))
        # A source has no declared input port, so port 0 is created empty just
        # to give the starter channel somewhere to attach.
        source_handler.context.input_manager.add_input_port.assert_called_once_with(
            port_id=PortIdentity(0, False),
            schema=Schema(),
            storage_uris=[],
            partitionings=[],
        )

    def test_source_registers_the_starter_channel_on_port_zero(self, source_handler):
        asyncio.run(source_handler.start_worker(EmptyRequest()))
        source_handler.context.input_manager.register_input.assert_called_once_with(
            SOURCE_STARTER_CHANNEL, PortIdentity(0, False)
        )

    def test_source_creates_the_port_before_registering_the_channel(
        self, source_handler
    ):
        asyncio.run(source_handler.start_worker(EmptyRequest()))
        # register_input ends with `self._ports[port_id].add_channel(...)`, so
        # the port has to exist first; doing these two in the other order
        # raises KeyError on a real InputManager.
        calls = [name for name, _, _ in source_handler.context.input_manager.mock_calls]
        assert calls.index("add_input_port") < calls.index("register_input")

    def test_source_sets_current_input_channel_id(self, source_handler):
        asyncio.run(source_handler.start_worker(EmptyRequest()))
        # The main loop reads current_input_channel_id while processing the
        # markers below, so it must already point at the starter channel.
        assert source_handler.context.current_input_channel_id == SOURCE_STARTER_CHANNEL

    @pytest.mark.timeout(2)
    def test_source_enqueues_start_channel_then_end_channel(self, source_handler):
        asyncio.run(source_handler.start_worker(EmptyRequest()))
        elements = _drain(source_handler.context.input_queue)

        # The whole point of the source branch: a StartChannel marker that is
        # not alignment-gated, immediately followed by a port-aligned
        # EndChannel that closes the synthetic port and drives the source to
        # completion. Order, alignment types and the no-reply command id are
        # all part of the contract.
        assert [_summarize(element) for element in elements] == [
            (
                SOURCE_STARTER_CHANNEL,
                "StartChannel",
                EmbeddedControlMessageType.NO_ALIGNMENT,
                {WORKER_ID},
                "StartChannel",
                NO_REPLY_COMMAND_ID,
            ),
            (
                SOURCE_STARTER_CHANNEL,
                "EndChannel",
                EmbeddedControlMessageType.PORT_ALIGNMENT,
                {WORKER_ID},
                "EndChannel",
                NO_REPLY_COMMAND_ID,
            ),
        ]

    @pytest.mark.timeout(2)
    def test_source_markers_carry_an_empty_scope(self, source_handler):
        asyncio.run(source_handler.start_worker(EmptyRequest()))
        # A self-addressed marker propagates nowhere, so the scope stays empty.
        assert [
            e.payload.scope for e in _drain(source_handler.context.input_queue)
        ] == [
            [],
            [],
        ]

    @pytest.mark.timeout(2)
    def test_source_markers_carry_an_unwrappable_empty_request(self, source_handler):
        asyncio.run(source_handler.start_worker(EmptyRequest()))
        # AsyncRpcServer unwraps the payload with get_one_of before dispatching.
        # Leaving the oneof unset delivers None to the StartChannel/EndChannel
        # handlers, and betterproto's __eq__ cannot see the difference — so the
        # request has to be checked through the same unwrapping the server does.
        for element in _drain(source_handler.context.input_queue):
            (command,) = element.payload.command_mapping.values()
            assert get_one_of(command.command) == EmptyRequest()

    def test_source_does_not_start_mat_reader_threads(self, source_handler):
        asyncio.run(source_handler.start_worker(EmptyRequest()))
        input_manager = source_handler.context.input_manager
        input_manager.start_input_port_mat_reader_threads.assert_not_called()

    def test_source_branch_is_checked_before_the_mat_reader_branch(self):
        # is_source wins even when reader threads exist: the elif is never
        # evaluated, so the predicate itself must go unqueried.
        handler = _build_handler(
            is_source=True,
            mat_reader_threads={PortIdentity(0, False): [MagicMock()]},
        )
        asyncio.run(handler.start_worker(EmptyRequest()))
        input_manager = handler.context.input_manager
        input_manager.get_input_port_mat_reader_threads.assert_not_called()
        input_manager.start_input_port_mat_reader_threads.assert_not_called()
        assert handler.context.input_queue.size() == 2

    # --- materialization-reader branch -----------------------------------

    def test_mat_reader_branch_starts_the_reader_threads(self, mat_reader_handler):
        asyncio.run(mat_reader_handler.start_worker(EmptyRequest()))
        input_manager = mat_reader_handler.context.input_manager
        input_manager.start_input_port_mat_reader_threads.assert_called_once_with()

    def test_mat_reader_branch_skips_all_source_effects(self, mat_reader_handler):
        asyncio.run(mat_reader_handler.start_worker(EmptyRequest()))
        context = mat_reader_handler.context
        # A non-source worker is driven to RUNNING by its upstream's channel
        # markers, not by this handler; fabricating a port or a starter channel
        # here would inject a second, bogus input.
        context.state_manager.transit_to.assert_not_called()
        context.input_manager.add_input_port.assert_not_called()
        context.input_manager.register_input.assert_not_called()
        assert context.current_input_channel_id is None
        assert context.input_queue.size() == 0

    # --- fall-through ----------------------------------------------------

    def test_fall_through_does_nothing(self, fall_through_handler):
        asyncio.run(fall_through_handler.start_worker(EmptyRequest()))
        context = fall_through_handler.context
        input_manager = context.input_manager
        # get_input_port_mat_reader_threads returns a mapping, and the branch
        # is guarded by its truthiness: an empty mapping means there is nothing
        # to read, so the predicate is consulted but every effect is skipped.
        input_manager.get_input_port_mat_reader_threads.assert_called_once_with()
        context.state_manager.transit_to.assert_not_called()
        input_manager.add_input_port.assert_not_called()
        input_manager.register_input.assert_not_called()
        input_manager.start_input_port_mat_reader_threads.assert_not_called()
        assert context.current_input_channel_id is None
        assert context.input_queue.size() == 0

    # --- response --------------------------------------------------------

    @pytest.mark.parametrize(
        "handler_fixture",
        ["source_handler", "mat_reader_handler", "fall_through_handler"],
    )
    def test_returns_the_state_paired_with_its_version(self, handler_fixture, request):
        handler = request.getfixturevalue(handler_fixture)
        result = asyncio.run(handler.start_worker(EmptyRequest()))
        # Every branch reports through the same atomic read, so the version
        # must travel with the state rather than be dropped or defaulted.
        assert result == WorkerStateResponse(
            WorkerState.RUNNING, state_version=STATE_VERSION
        )
        handler.context.state_manager.get_state_with_version.assert_called_once_with()

    def test_reports_whatever_state_the_state_manager_holds(self, fall_through_handler):
        # A non-source worker with no materialized inputs is still READY here;
        # the handler must not substitute RUNNING for it.
        state_manager = fall_through_handler.context.state_manager
        state_manager.get_state_with_version.return_value = (WorkerState.READY, 3)
        result = asyncio.run(fall_through_handler.start_worker(EmptyRequest()))
        assert result == WorkerStateResponse(WorkerState.READY, state_version=3)

    # --- real collaborators ----------------------------------------------

    def test_source_branch_wires_up_a_real_input_manager(self):
        handler = _build_real_handler(is_source=True, initial_state=WorkerState.READY)
        result = asyncio.run(handler.start_worker(EmptyRequest()))
        input_manager = handler.context.input_manager

        # READY -> RUNNING is a real transition, so the version advances off 0.
        assert result == WorkerStateResponse(WorkerState.RUNNING, state_version=1)
        assert handler.context.input_queue.size() == 2

        # The synthetic port really exists and really owns the starter channel.
        # InputManager has no public reader for the whole port map, so only the
        # exhaustive port check reaches for _ports; the rest goes through the
        # public accessors.
        assert list(input_manager._ports) == [PortIdentity(0, False)]
        assert list(input_manager.get_all_channel_ids()) == [SOURCE_STARTER_CHANNEL]
        assert input_manager.get_port_id(SOURCE_STARTER_CHANNEL) == PortIdentity(
            0, False
        )
        port = input_manager.get_port(PortIdentity(0, False))
        assert port.get_channels() == {SOURCE_STARTER_CHANNEL}
        assert port.get_schema() == Schema()

        # add_input_port records a reader-runnable list for every port it
        # creates, even with no materialization URIs, so the real mapping is
        # {port: []} and not {}. The mock-based tests above model the guard as
        # "are there reader threads?"; on a real InputManager it is really
        # "has any input port been added?".
        assert input_manager.get_input_port_mat_reader_threads() == {
            PortIdentity(0, False): []
        }

    def test_rejected_transition_leaves_nothing_enqueued(self):
        # UNINITIALIZED only permits READY, so a source told to start before it
        # finished initializing cannot reach RUNNING.
        handler = _build_real_handler(
            is_source=True, initial_state=WorkerState.UNINITIALIZED
        )
        with pytest.raises(InvalidTransitionException):
            asyncio.run(handler.start_worker(EmptyRequest()))

        context = handler.context
        # transit_to runs ahead of both put calls, so the failure is clean. A
        # future reordering that enqueued first would strand two markers in the
        # queue with the state never having advanced.
        assert context.input_queue.size() == 0
        assert context.state_manager.get_state_with_version() == (
            WorkerState.UNINITIALIZED,
            0,
        )
        assert context.current_input_channel_id is None
        # "Clean" has to cover the InputManager too, not just the queue: doing
        # the port and channel wiring ahead of the transition would leave a
        # worker that is still UNINITIALIZED owning a phantom port 0 and a
        # phantom starter channel.
        assert context.input_manager.get_input_port_mat_reader_threads() == {}
        assert list(context.input_manager.get_all_channel_ids()) == []

    def test_double_start_enqueues_a_second_pair_of_markers(self):
        # THIS PINS OBSERVED CURRENT BEHAVIOR — it is not an assertion that the
        # behavior is correct. Starting an already-running source does not
        # raise, because StateManager.transit_to returns early when the target
        # equals the current state: RUNNING -> RUNNING is a silent no-op that
        # does not even bump the version. The handler carries no idempotency
        # guard of its own, so a second pair of markers is appended — including
        # a second PORT_ALIGNMENT EndChannel on the same port, which may or may
        # not be intended. Revisit and rewrite this test if the handler ever
        # gains such a guard.
        handler = _build_real_handler(is_source=True, initial_state=WorkerState.READY)
        first = asyncio.run(handler.start_worker(EmptyRequest()))
        assert handler.context.input_queue.size() == 2

        second = asyncio.run(handler.start_worker(EmptyRequest()))
        assert handler.context.input_queue.size() == 4
        # The no-op transition leaves the logical clock untouched, so the two
        # responses are indistinguishable.
        assert second == first
        assert second.state_version == first.state_version == 1
        # Port and channel registration, unlike the queue, is idempotent.
        assert list(handler.context.input_manager.get_all_channel_ids()) == [
            SOURCE_STARTER_CHANNEL
        ]

    def test_mat_reader_branch_leaves_a_real_state_clock_untouched(self):
        # The mocked counterpart above pins this as "transit_to is not called";
        # against a real StateManager the same guarantee is visible from the
        # outside, as a worker still READY at version 0. Reading a materialized
        # port does not start the worker -- the main loop transitions on the
        # first arriving tuple instead.
        input_manager = MagicMock(spec=InputManager)
        input_manager.get_input_port_mat_reader_threads.return_value = {
            PortIdentity(0, False): [MagicMock()]
        }
        handler = _build_real_handler(
            is_source=False,
            initial_state=WorkerState.READY,
            input_manager=input_manager,
        )

        result = asyncio.run(handler.start_worker(EmptyRequest()))

        input_manager.start_input_port_mat_reader_threads.assert_called_once_with()
        assert result == WorkerStateResponse(WorkerState.READY, state_version=0)
        assert handler.context.input_queue.size() == 0
        assert handler.context.current_input_channel_id is None

    def test_reports_a_real_paused_state_untouched(self):
        # For a non-source worker the handler is a pure reporter, and the state
        # it reports is read after the branch rather than snapshotted before
        # it. A real StateManager -- unlike a mock returning a constant -- makes
        # a hoisted or stale read observable, since PAUSED at version 1 can only
        # come from the live clock.
        input_manager = MagicMock(spec=InputManager)
        input_manager.get_input_port_mat_reader_threads.return_value = {}
        handler = _build_real_handler(
            is_source=False,
            initial_state=WorkerState.READY,
            input_manager=input_manager,
        )
        handler.context.state_manager.transit_to(WorkerState.PAUSED)

        result = asyncio.run(handler.start_worker(EmptyRequest()))

        assert result == WorkerStateResponse(WorkerState.PAUSED, state_version=1)

    def test_markers_are_routed_to_the_data_sub_queue(self):
        handler = _build_real_handler(is_source=True, initial_state=WorkerState.READY)
        asyncio.run(handler.start_worker(EmptyRequest()))
        queue = handler.context.input_queue
        # ECMElements are data-queue traffic, not control traffic, so a source
        # worker's own start/end markers are gated by disable_data — pause and
        # backpressure — unlike direct control messages, which keep flowing on
        # the control sub-queue.
        assert (queue.size_control(), queue.size_data()) == (0, 2)
