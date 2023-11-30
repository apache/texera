from core.architecture.handlers.actorcommand.actor_handler_base import (
    ActorCommandHandler,
)
from core.architecture.managers.internal_queue_manager import (
    InternalQueueManager,
    DisableType,
)
from core.models.internal_queue import ControlElement
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.worker import ControlCommandV2, NoOpV2

from proto.edu.uci.ics.amber.engine.common import (
    Backpressure,
    ActorVirtualIdentity,
    ControlPayloadV2,
    ControlInvocationV2,
)


class BackpressureHandler(ActorCommandHandler):
    cmd = Backpressure

    def __call__(
        self,
        command: Backpressure,
        input_queue_manager: InternalQueueManager,
        *args,
        **kwargs
    ):
        if command.enable_backpressure:
            input_queue_manager.disable_data(DisableType.DISABLE_BY_BACKPRESSURE)
        else:
            input_queue_manager.enable_data(DisableType.DISABLE_BY_BACKPRESSURE)
            input_queue_manager.put(
                ControlElement(
                    tag=ActorVirtualIdentity(""),
                    payload=set_one_of(
                        ControlPayloadV2,
                        ControlInvocationV2(-1, set_one_of(ControlCommandV2, NoOpV2())),
                    ),
                )
            )

        return None
