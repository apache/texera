from collections import defaultdict
from enum import Enum

from typing import Set, Dict, List

from core.models import InternalQueue
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity


class PauseType(Enum):
    NO_PAUSE = 0
    USER_PAUSE = 1
    SCHEDULER_TIME_SLOT_EXPIRED_PAUSE = 2
    BACKPRESSURE_PAUSE = 3


class PauseManager:
    """
    Manage pause states.
    """

    def __init__(self, input_queue: InternalQueue):
        self._input_queue: InternalQueue = input_queue
        self._global_pauses: Set[PauseType] = set()
        self._specific_input_pauses: Dict[PauseType, Set[ActorVirtualIdentity]] = \
            defaultdict(set)

    def pause(self, pause_type: PauseType) -> None:
        self._global_pauses.add(pause_type)
        self._input_queue.disable_data()

    def pause_input_channel(self, pause_type: PauseType, inputs: List[
        ActorVirtualIdentity]) -> None:
        for input in inputs:
            self._specific_input_pauses[pause_type].add(input)
            self._input_queue.disable_data()  # for now we do not have specific data
            # queue for Python side. We disable all and the only data queue.

    def resume(self, pause_type) -> None:
        self._global_pauses.remove(pause_type)
        # del self._specific_input_pauses[pause_type]

        # still globally paused no action, don't need to resume anything
        if self._global_pauses:
            return

        # global pause is empty, specific input pause is also empty, resume all
        if not self._specific_input_pauses:
            self._input_queue.enable_data()
            return

        # need to resume specific input channels
        # currently no use case in Python side. Not implemented.
        raise NotImplementedError()

    def is_paused(self) -> bool:
        return bool(self._global_pauses)
