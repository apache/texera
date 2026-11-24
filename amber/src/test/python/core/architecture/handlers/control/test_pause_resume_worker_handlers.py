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

import pytest

from core.architecture.handlers.control.pause_worker_handler import PauseWorkerHandler
from core.architecture.handlers.control.resume_worker_handler import (
    ResumeWorkerHandler,
)
from core.architecture.managers.context import WORKER_STATE_TRANSITIONS
from core.architecture.managers.pause_manager import PauseManager, PauseType
from core.architecture.managers.state_manager import StateManager
from core.models.internal_queue import InternalQueue
from proto.org.apache.texera.amber.engine.architecture.rpc import (
    EmptyRequest,
    WorkerStateResponse,
)
from proto.org.apache.texera.amber.engine.architecture.worker import WorkerState


def _context(initial_state: WorkerState) -> SimpleNamespace:
    """The real collaborators pause/resume touch, wired as Context wires them:
    the PauseManager shares the context's queue and state manager."""
    queue = InternalQueue()
    state_manager = StateManager(WORKER_STATE_TRANSITIONS, initial_state)
    return SimpleNamespace(
        input_queue=queue,
        state_manager=state_manager,
        pause_manager=PauseManager(queue, state_manager=state_manager),
    )


def _pause(context) -> WorkerStateResponse:
    return asyncio.run(PauseWorkerHandler(context).pause_worker(EmptyRequest()))


def _resume(context) -> WorkerStateResponse:
    return asyncio.run(ResumeWorkerHandler(context).resume_worker(EmptyRequest()))


def _data_disabled_by_pause(context) -> bool:
    return (
        InternalQueue.DisableType.DISABLE_BY_PAUSE in context.input_queue._queue_state
    )


class TestPauseWorkerHandler:
    @pytest.mark.parametrize("state", [WorkerState.RUNNING, WorkerState.READY])
    def test_pauses_a_running_or_ready_worker(self, state):
        context = _context(state)
        response = _pause(context)

        assert response.state == WorkerState.PAUSED
        assert context.state_manager.get_current_state() == WorkerState.PAUSED
        assert context.pause_manager.is_paused()
        assert _data_disabled_by_pause(context)

    def test_reports_the_state_version_after_the_transition(self):
        context = _context(WorkerState.RUNNING)
        _, before = context.state_manager.get_state_with_version()

        response = _pause(context)

        _, after = context.state_manager.get_state_with_version()
        assert after != before
        assert response.state_version == after

    def test_pausing_twice_keeps_the_worker_paused_and_the_version(self):
        context = _context(WorkerState.RUNNING)
        first = _pause(context)
        second = _pause(context)

        assert second.state == WorkerState.PAUSED
        assert second.state_version == first.state_version

    def test_does_not_move_a_completed_worker(self):
        # COMPLETED has no outgoing transitions; the pause is recorded, the
        # state is left alone and reported as is.
        context = _context(WorkerState.COMPLETED)
        response = _pause(context)

        assert response.state == WorkerState.COMPLETED
        assert context.state_manager.get_current_state() == WorkerState.COMPLETED
        assert not context.pause_manager.is_paused()


class TestResumeWorkerHandler:
    def test_resumes_a_user_paused_worker(self):
        context = _context(WorkerState.RUNNING)
        _pause(context)

        response = _resume(context)

        assert response.state == WorkerState.RUNNING
        assert context.state_manager.get_current_state() == WorkerState.RUNNING
        assert not context.pause_manager.is_paused()
        assert not _data_disabled_by_pause(context)

    def test_a_paused_ready_worker_resumes_to_running(self):
        context = _context(WorkerState.READY)
        _pause(context)

        assert _resume(context).state == WorkerState.RUNNING

    def test_reports_the_state_version_after_the_transition(self):
        context = _context(WorkerState.RUNNING)
        _pause(context)

        response = _resume(context)

        _, version = context.state_manager.get_state_with_version()
        assert response.state_version == version

    def test_stays_paused_while_another_pause_is_held(self):
        # A user resume lifts only the user's pause; an exception pause keeps
        # the worker paused and its data disabled.
        context = _context(WorkerState.RUNNING)
        _pause(context)
        context.pause_manager.pause(PauseType.EXCEPTION_PAUSE)

        response = _resume(context)

        assert response.state == WorkerState.PAUSED
        assert context.pause_manager.is_paused()
        assert _data_disabled_by_pause(context)

    def test_resuming_a_running_worker_leaves_it_running(self):
        context = _context(WorkerState.RUNNING)
        _, before = context.state_manager.get_state_with_version()

        response = _resume(context)

        assert response.state == WorkerState.RUNNING
        assert response.state_version == before

    def test_does_not_move_a_completed_worker(self):
        context = _context(WorkerState.COMPLETED)
        assert _resume(context).state == WorkerState.COMPLETED
