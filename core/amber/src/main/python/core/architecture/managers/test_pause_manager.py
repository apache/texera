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

from core.architecture.managers import StateManager
from core.architecture.managers.pause_manager import PauseManager, PauseType
from core.models import InternalQueue
from proto.edu.uci.ics.amber.engine.architecture.worker import WorkerState


class TestPauseManager:
    @pytest.fixture
    def input_queue(self):
        return InternalQueue()

    @pytest.fixture
    def state_manager(self):
        return StateManager(
            {
                WorkerState.UNINITIALIZED: {WorkerState.READY},
                WorkerState.READY: {WorkerState.PAUSED, WorkerState.RUNNING},
                WorkerState.RUNNING: {WorkerState.PAUSED, WorkerState.COMPLETED},
                WorkerState.PAUSED: {WorkerState.RUNNING},
                WorkerState.COMPLETED: set(),
            },
            WorkerState.READY,  # initial state set to READY for testing purpose
        )

    @pytest.fixture
    def pause_manager(self, input_queue, state_manager):
        return PauseManager(input_queue, state_manager)

    def test_it_can_init(self, pause_manager):
        pass

    def test_it_is_not_paused_initially(self, pause_manager):
        assert not pause_manager.is_paused()

    def test_it_can_be_paused_and_resumed(self, pause_manager):
        pause_manager.pause(PauseType.USER_PAUSE)
        assert pause_manager.is_paused()
        pause_manager.resume(PauseType.USER_PAUSE)
        assert not pause_manager.is_paused()

    def test_it_can_be_paused_when_paused(self, pause_manager):
        pause_manager.pause(PauseType.USER_PAUSE)
        assert pause_manager.is_paused()
        pause_manager.pause(PauseType.USER_PAUSE)
        assert pause_manager.is_paused()
        pause_manager.resume(PauseType.USER_PAUSE)
        assert not pause_manager.is_paused()

    def test_it_can_be_resumed_when_resumed(self, pause_manager):
        pause_manager.pause(PauseType.USER_PAUSE)
        assert pause_manager.is_paused()
        pause_manager.resume(PauseType.USER_PAUSE)
        assert not pause_manager.is_paused()
        pause_manager.resume(PauseType.USER_PAUSE)
        assert not pause_manager.is_paused()
