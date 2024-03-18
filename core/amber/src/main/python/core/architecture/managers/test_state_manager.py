import pytest

from core.architecture.managers.state_manager import (
    InvalidStateException,
    InvalidTransitionException,
    StateManager,
)
from proto.edu.uci.ics.amber.engine.architecture.worker import WorkerState


class TestStateManager:
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
            WorkerState.UNINITIALIZED,
        )

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
