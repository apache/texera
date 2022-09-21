from enum import Enum


class PauseType(Enum):
    NO_PAUSE = 0
    USER_PAUSE = 1
    SCHEDULER_TIME_SLOT_EXPIRED_PAUSE = 2


class PauseManager:
    """
    Manage pause states.
    """

    def __init__(self):
        self._pause_invocations = dict()

    def record_request(self, pause_type: PauseType, enable_pause: bool) -> None:
        self._pause_invocations[pause_type] = enable_pause

    def get_pause_status_by_type(self, pause_type: PauseType) -> bool:
        return self._pause_invocations.get(pause_type, False)

    def is_paused(self) -> bool:
        return any(pause_status for pause_status in self._pause_invocations.values())
