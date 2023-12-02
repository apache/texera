from abc import ABC

from core.models import InternalQueue
from proto.edu.uci.ics.amber.engine.common import ActorCommand


class ActorCommandHandler(ABC):
    cmd: ActorCommand = None

    def __call__(
        self, command: ActorCommand, input_queue: InternalQueue, *args, **kwargs
    ) -> None:
        pass
