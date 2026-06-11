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
