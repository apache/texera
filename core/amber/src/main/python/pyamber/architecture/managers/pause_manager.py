from enum import Enum


class PauseState(Enum):
    NO_PAUSE = 0
    PAUSED = 1


class PauseManager:
    """
    Manage pause states.
    """

    def __init__(self):
        self._pause_state = PauseState.NO_PAUSE

    def pause(self) -> None:
        """
        Transit to PAUSED state from any state.
        """
        self._pause_state = PauseState.PAUSED

    def resume(self) -> None:
        """
        Transit to NO_PAUSE state from any state.
        """
        self._pause_state = PauseState.NO_PAUSE

    def is_paused(self) -> bool:
        """
        Check if it is at PAUSED state.
        :return: bool, indicating whether paused or not.
        """
        return self._pause_state == PauseState.PAUSED
