from collections import defaultdict
from enum import Enum

from typing import Set, Dict

from loguru import logger

from . import state_manager
from proto.edu.uci.ics.amber.engine.architecture.worker import WorkerState
from proto.edu.uci.ics.amber.core import ChannelIdentity
from ...models import InternalQueue


class PauseType(Enum):
    NO_PAUSE = 0
    USER_PAUSE = 1
    SCHEDULER_TIME_SLOT_EXPIRED_PAUSE = 2
    DEBUG_PAUSE = 3
    EXCEPTION_PAUSE = 4
    MARKER_PAUSE = 5


class PauseManager:
    """
    Manage pause states.
    """

    def __init__(
        self,
        input_queue: InternalQueue,
        state_manager: state_manager.StateManager,
    ):
        self._input_queue: InternalQueue = input_queue
        self._global_pauses: Set[PauseType] = set()
        self._specific_input_pauses: Dict[PauseType, Set[ChannelIdentity]] = (
            defaultdict(set)
        )
        self._state_manager = state_manager

    def pause(self, pause_type: PauseType, change_state=True) -> None:
        logger.debug("pause by " + str(pause_type))
        self._global_pauses.add(pause_type)
        self._input_queue.disable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)

        if change_state and self._state_manager.confirm_state(
            WorkerState.RUNNING, WorkerState.READY
        ):
            self._state_manager.transit_to(WorkerState.PAUSED)

    def pause_input_channel(
        self, pause_type: PauseType, channel_id: ChannelIdentity
    ) -> None:
        self._specific_input_pauses[pause_type].add(channel_id)
        self._input_queue.disable(channel_id)

    def resume(self, pause_type: PauseType, change_state=True) -> None:
        if pause_type in self._global_pauses:
            self._global_pauses.remove(pause_type)
        if pause_type in self._specific_input_pauses:
            # need to resume specific input channels
            for channel_id in self._specific_input_pauses[pause_type]:
                self._input_queue.enable(channel_id)
            del self._specific_input_pauses[pause_type]

        # still globally paused no action, don't need to resume anything
        if self._global_pauses:
            return

        # global pause is empty, specific input pause is also empty, resume all
        if not self._specific_input_pauses:
            self._input_queue.enable_data(InternalQueue.DisableType.DISABLE_BY_PAUSE)
            if change_state and self._state_manager.confirm_state(WorkerState.PAUSED):
                self._state_manager.transit_to(WorkerState.RUNNING)
            return

    def is_paused(self) -> bool:
        return bool(self._global_pauses) and self._state_manager.confirm_state(
            WorkerState.PAUSED
        )
