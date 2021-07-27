from __future__ import annotations

from enum import Enum


class PauseState(Enum):
    NO_PAUSE = 0
    PAUSED = 1


class PauseManager:

    def __init__(self):
        self._pause_state = PauseState.NO_PAUSE

    def pause(self):
        self._pause_state = PauseState.PAUSED

    def resume(self):
        self._pause_state = PauseState.NO_PAUSE

    def is_paused(self) -> bool:
        return self._pause_state == PauseState.PAUSED
