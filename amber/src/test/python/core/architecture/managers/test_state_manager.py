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

import pytest

from core.architecture.managers.context import WORKER_STATE_TRANSITIONS
from core.architecture.managers.state_manager import (
    InvalidStateException,
    InvalidTransitionException,
    StateManager,
)
from proto.org.apache.texera.amber.engine.architecture.worker import WorkerState


class TestStateManager:
    @pytest.fixture
    def state_manager(self):
        return StateManager(WORKER_STATE_TRANSITIONS, WorkerState.UNINITIALIZED)

    def test_it_can_init(self, state_manager):
        pass

    def test_it_can_transit_to_defined_state(self, state_manager):
        state_manager.assert_state(WorkerState.UNINITIALIZED)
        for state in [
            WorkerState.READY,
            WorkerState.PAUSED,
            WorkerState.RUNNING,
            WorkerState.COMPLETED,
        ]:
            state_manager.transit_to(state)
            assert state_manager.confirm_state(state)
            state_manager.assert_state(state)

    def test_it_raises_exception_when_transit_to_undefined_state(self, state_manager):
        state_manager.assert_state(WorkerState.UNINITIALIZED)
        for state in [WorkerState.READY, WorkerState.PAUSED]:
            state_manager.transit_to(state)
            assert state_manager.confirm_state(state)
            state_manager.assert_state(state)
        with pytest.raises(InvalidTransitionException):
            state_manager.transit_to(WorkerState.READY)

    def test_it_raises_exception_when_asserting_a_different_state(self, state_manager):
        state_manager.assert_state(WorkerState.UNINITIALIZED)
        for state in [WorkerState.READY, WorkerState.PAUSED]:
            state_manager.transit_to(state)
            assert state_manager.confirm_state(state)
            state_manager.assert_state(state)

        with pytest.raises(InvalidStateException):
            state_manager.assert_state(WorkerState.COMPLETED)

    def test_it_can_transit_directly_from_ready_to_completed(self, state_manager):
        # A worker can complete directly from READY without first entering
        # RUNNING. This path is taken when there is nothing to process
        # (upstream signals end-of-stream before any data arrives).
        state_manager.transit_to(WorkerState.READY)
        state_manager.transit_to(WorkerState.COMPLETED)
        state_manager.assert_state(WorkerState.COMPLETED)

    def test_state_version_starts_at_zero(self, state_manager):
        assert state_manager.get_state_version() == 0

    def test_state_version_bumps_on_every_successful_transition(self, state_manager):
        # The controller relies on this monotonic version to order Python-worker
        # state reports causally; without it, RUNNING -> PAUSED -> RUNNING during
        # reconfiguration would be dropped as stale. Mirrors the Scala StateManager.
        assert state_manager.get_state_version() == 0
        state_manager.transit_to(WorkerState.READY)
        assert state_manager.get_state_version() == 1
        state_manager.transit_to(WorkerState.RUNNING)
        assert state_manager.get_state_version() == 2
        state_manager.transit_to(WorkerState.COMPLETED)
        assert state_manager.get_state_version() == 3

    def test_state_version_does_not_bump_on_noop_self_transition(self, state_manager):
        state_manager.transit_to(WorkerState.READY)
        before = state_manager.get_state_version()
        state_manager.transit_to(WorkerState.READY)  # no-op
        assert state_manager.get_state_version() == before

    def test_state_version_does_not_bump_on_rejected_transition(self, state_manager):
        # UNINITIALIZED -> RUNNING is illegal (must pass through READY).
        with pytest.raises(InvalidTransitionException):
            state_manager.transit_to(WorkerState.RUNNING)
        assert state_manager.get_state_version() == 0

    def test_get_state_with_version_returns_matching_pair(self, state_manager):
        # Report sites read state and version through this single accessor so the
        # pair can never come from two different transitions. Mirrors the Scala
        # StateManager's getStateWithVersion.
        assert state_manager.get_state_with_version() == (
            WorkerState.UNINITIALIZED,
            0,
        )
        state_manager.transit_to(WorkerState.READY)
        assert state_manager.get_state_with_version() == (WorkerState.READY, 1)
        state_manager.transit_to(WorkerState.RUNNING)
        assert state_manager.get_state_with_version() == (WorkerState.RUNNING, 2)
