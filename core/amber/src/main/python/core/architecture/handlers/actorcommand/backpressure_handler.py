from core.architecture.handlers.actorcommand.actor_handler_base import (
    ActorCommandHandler,
)

from core.models.internal_queue import ControlElement, InternalQueue
from core.util import set_one_of
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    ControlInvocation,
    ControlRequest,
    EmptyRequest,
    AsyncRpcContext,
)

from proto.edu.uci.ics.amber.engine.common import (
    Backpressure,
    ActorVirtualIdentity,
    ControlPayloadV2,
)


class BackpressureHandler(ActorCommandHandler):
    cmd = Backpressure

    def __call__(
        self, command: Backpressure, input_queue: InternalQueue, *args, **kwargs
    ):
        if command.enable_backpressure:
            input_queue.disable_data(InternalQueue.DisableType.DISABLE_BY_BACKPRESSURE)
        else:
            input_queue.enable_data(InternalQueue.DisableType.DISABLE_BY_BACKPRESSURE)
            input_queue.put(
                ControlElement(
                    tag=ActorVirtualIdentity(""),
                    payload=set_one_of(
                        ControlPayloadV2,
                        ControlInvocation(
                            "NoOperation",
                            set_one_of(ControlRequest, EmptyRequest()),
                            AsyncRpcContext(),
                            -1,
                        ),
                    ),
                )
            )

        return None
