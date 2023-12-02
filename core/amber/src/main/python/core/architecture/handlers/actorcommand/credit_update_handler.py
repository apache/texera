from core.architecture.handlers.actorcommand.actor_handler_base import (
    ActorCommandHandler,
)
from core.models import InternalQueue
from proto.edu.uci.ics.amber.engine.common import CreditUpdate


class CreditUpdateHandler(ActorCommandHandler):
    cmd = CreditUpdate

    def __call__(
        self, command: CreditUpdate, input_queue: InternalQueue, *args, **kwargs
    ):
        # do nothing
        return None
