from typing import Dict, Set, Tuple, Union

from typing_extensions import T


class InvalidStateException(Exception):
    pass


class InvalidTransitionException(Exception):
    pass


class StateManager:
    """
    A generalized StateManager that provides APIs for state transition, assertion,
    and confirmation.
    """

    def __init__(self, state_transition_graph: Dict[T, Set[T]], initial_state: T):
        self._state_transition_graph = state_transition_graph
        self._current_state: T = initial_state
        self._state_stack: list[T] = list()
        self._state_stack.append(initial_state)

    def assert_state(self, state: T) -> None:
        """
        Assert the current state to be the expected state, raise exception if otherwise.
        :param state: the expected state.
        """
        if self._current_state != state:
            raise InvalidStateException(
                f"Excepted state = {state} but current state = {self._current_state}"
            )

    def confirm_state(self, *states: Union[T, Tuple[T]]) -> bool:
        """
        Check if current state is in one of the states.

        :param states: Union[T, Tuple[T]], a series of states to be checked.
        :return: bool
        """
        return any(self._current_state == state for state in states)

    def transit_to(self, state: T, discard_old_states: bool = True) -> None:
        """
        Transit the current state into the target state.
        If discard_old_states is True, remove states in the stack.

        :param state: T, the target state to transit to.
        :param discard_old_states: bool, whether remove stacked states or not.
        :return:
        """

        # do nothing if the current state is already the target state
        if state == self._current_state:
            return

        if discard_old_states:
            self._state_stack.clear()

        self._state_stack.append(state)

        if state not in self._state_transition_graph.get(self._current_state, set()):
            raise InvalidTransitionException(
                f"Cannot transit from {self._current_state} to {state}"
            )

        self._current_state = state

    def back_to_previous_state(self) -> None:
        """
        Revert to the previous state saved in the stack.
        """
        if len(self._state_stack) == 0:
            raise InvalidTransitionException(
                f"There is no previous state for {self._current_state}"
            )
        self._current_state = self._state_stack.pop(-1)

    def get_current_state(self) -> T:
        """
        Return the current state.
        :return:
        """
        return self._current_state
