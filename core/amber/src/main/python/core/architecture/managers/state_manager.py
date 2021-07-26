from proto.edu.uci.ics.amber.engine.architecture.worker import WorkerState


class InvalidStateException(Exception):
    pass


class InvalidTransitionException(Exception):
    pass


class StateManager:

    def __init__(self, state_transition_graph, initial_state: WorkerState):
        self._state_transition_graph = state_transition_graph
        self._current_state: WorkerState = initial_state
        self._state_stack: list[WorkerState] = list()
        self._state_stack.append(initial_state)

    def assert_state(self, state: WorkerState):
        if self._current_state != state:
            raise InvalidStateException(f"except state = {state} but current state = {self._current_state}")

    def confirm_state(self, *states: WorkerState) -> bool:
        return any([self._current_state == state for state in states])

    def transit_to(self, state: WorkerState, discard_old_states: bool = True) -> None:
        if state == self._current_state:
            return

        if discard_old_states:
            self._state_stack.clear()

        self._state_stack.append(state)

        if state not in self._state_transition_graph.get(self._current_state, set()):
            raise InvalidTransitionException(f"cannot transit from {self._current_state} to {state}")

        self._current_state = state

    def back_to_previous_state(self) -> None:
        if len(self._state_stack) == 0:
            raise InvalidTransitionException(f"there is no previous state for {self._current_state}")
        self._current_state = self._state_stack.pop(-1)

    def get_current_state(self) -> WorkerState:
        return self._current_state
