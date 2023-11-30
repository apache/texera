from abc import ABC

from core.architecture.managers.internal_queue_manager import InternalQueueManager
from proto.edu.uci.ics.amber.engine.common import ActorCommand


class ActorCommandHandler(ABC):
    cmd: ActorCommand = None

    def __call__(
        self,
        command: ActorCommand,
        input_queue_manager: InternalQueueManager,
        *args,
        **kwargs
    ) -> None:
        pass
