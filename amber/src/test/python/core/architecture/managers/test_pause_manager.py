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
from core.models.internal_queue import DataElement
from core.models.payload import DataPayload
from proto.org.apache.texera.amber.core import ActorVirtualIdentity, ChannelIdentity
from proto.org.apache.texera.amber.engine.architecture.worker import WorkerState


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

    @staticmethod
    def _register_channel(input_queue, name: str) -> ChannelIdentity:
        """A channel only gets a sub-queue once something is enqueued on it, so
        go through the production put() path before pausing it."""
        channel = ChannelIdentity(
            ActorVirtualIdentity(name), ActorVirtualIdentity("self"), False
        )
        input_queue.put(DataElement(tag=channel, payload=DataPayload()))
        return channel

    def test_resume_of_one_type_keeps_worker_paused_while_another_is_held(
        self, pause_manager, state_manager
    ):
        # Two independent global pause holders. Releasing one must not resume
        # the worker -- resume() returns early while any global pause remains.
        pause_manager.pause(PauseType.USER_PAUSE)
        pause_manager.pause(PauseType.DEBUG_PAUSE)
        assert state_manager.confirm_state(WorkerState.PAUSED)

        pause_manager.resume(PauseType.USER_PAUSE)
        assert pause_manager.is_paused()
        assert state_manager.confirm_state(WorkerState.PAUSED)

        # resume() must release the type it was ASKED for, not just some
        # arbitrary holder: repeating the release of the already-released
        # USER_PAUSE is a no-op, and DEBUG_PAUSE still holds the worker.
        # Without this, `resume(X)` popping an arbitrary element off the pause
        # set is indistinguishable from popping X -- and an EXCEPTION_PAUSE
        # released by a user resume would silently restart a failed worker.
        pause_manager.resume(PauseType.USER_PAUSE)
        assert pause_manager.is_paused()
        assert state_manager.confirm_state(WorkerState.PAUSED)

        # Releasing the last holder does resume.
        pause_manager.resume(PauseType.DEBUG_PAUSE)
        assert not pause_manager.is_paused()
        assert state_manager.confirm_state(WorkerState.RUNNING)

    def test_global_pause_closes_the_data_queue_and_resume_reopens_it(
        self, pause_manager, input_queue
    ):
        # pause()/resume() gate the data queues wholesale, keyed by
        # DISABLE_BY_PAUSE. The channel is registered BEFORE the pause, which
        # is what makes the blanket disable observable at all: disable_data()
        # only iterates the sub-queues that exist when it runs, so a test that
        # registers afterwards cannot see it. Re-enabling under the wrong
        # DisableType key would leave a resumed worker's data queue shut for
        # good, i.e. a hang.
        self._register_channel(input_queue, "up")
        assert input_queue.is_data_enabled()

        pause_manager.pause(PauseType.USER_PAUSE)
        assert not input_queue.is_data_enabled()

        pause_manager.resume(PauseType.USER_PAUSE)
        assert input_queue.is_data_enabled()

    def test_channel_pause_blocks_the_global_resume(
        self, pause_manager, state_manager, input_queue
    ):
        # A per-channel pause outlives the global one: once the global pause is
        # released the pause set is empty, but a channel is still held, so the
        # blanket data re-enable and the PAUSED -> RUNNING transition are both
        # skipped. Note the channel is registered AFTER pause(), so pause()'s
        # own disable_data() found no sub-queues and did nothing here -- the
        # channel is closed only by pause_input_channel below.
        pause_manager.pause(PauseType.USER_PAUSE)
        channel = self._register_channel(input_queue, "up")
        pause_manager.pause_input_channel(PauseType.DEBUG_PAUSE, channel)
        assert state_manager.confirm_state(WorkerState.PAUSED)
        assert not input_queue.is_data_enabled()

        pause_manager.resume(PauseType.USER_PAUSE)
        assert state_manager.confirm_state(WorkerState.PAUSED)
        # The blanket re-enable really is skipped, not merely state-neutral.
        assert not input_queue.is_data_enabled()

        # Releasing the channel pause finally resumes.
        pause_manager.resume(PauseType.DEBUG_PAUSE)
        assert state_manager.confirm_state(WorkerState.RUNNING)
        assert input_queue.is_data_enabled()

    def test_releasing_one_channel_pause_reopens_that_channel(
        self, pause_manager, input_queue
    ):
        # Two channels held by two DIFFERENT pause types, and no global pause.
        # Releasing one type leaves the other still holding, so resume()
        # returns before the blanket enable_data() at the end of the method --
        # which means the released channel can only be open again because
        # resume() re-enabled it channel by channel. In the sibling test above,
        # that blanket enable masks the per-channel work entirely.
        first = self._register_channel(input_queue, "first")
        second = self._register_channel(input_queue, "second")
        pause_manager.pause_input_channel(PauseType.DEBUG_PAUSE, first)
        # ECM_PAUSE is the type main_loop actually uses for channel pauses.
        pause_manager.pause_input_channel(PauseType.ECM_PAUSE, second)
        assert not input_queue.is_data_enabled()

        pause_manager.resume(PauseType.DEBUG_PAUSE)
        assert input_queue.is_data_enabled()

    def test_pause_with_change_state_false_leaves_the_state_alone(
        self, pause_manager, state_manager
    ):
        # change_state=False is the "gate the queues but do not touch the
        # reported worker state" path. No production caller overrides the
        # default today, so this pins the parameter's contract rather than a
        # live behaviour -- but without it the guard is vacuously true and
        # deleting `change_state and` from either method changes nothing.
        pause_manager.pause(PauseType.USER_PAUSE, change_state=False)
        assert state_manager.confirm_state(WorkerState.READY)

    def test_resume_with_change_state_false_reopens_the_queue_but_not_the_state(
        self, pause_manager, state_manager, input_queue
    ):
        self._register_channel(input_queue, "up")
        pause_manager.pause(PauseType.USER_PAUSE)
        assert state_manager.confirm_state(WorkerState.PAUSED)
        assert not input_queue.is_data_enabled()

        pause_manager.resume(PauseType.USER_PAUSE, change_state=False)
        # The state label deliberately lags...
        assert state_manager.confirm_state(WorkerState.PAUSED)
        # ...but the worker really is running again: no pause holder remains
        # and the data queue is open. is_paused() must follow the pause set,
        # not the stale label, or a caller polling it would wait forever.
        assert input_queue.is_data_enabled()
        assert not pause_manager.is_paused()
