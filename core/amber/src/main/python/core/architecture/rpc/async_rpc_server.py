import grpclib.const
from loguru import logger

from core.architecture.managers.context import Context
from core.architecture.rpc.async_rpc_handler_initializer import (
    AsyncRPCHandlerInitializer,
)
from core.models.internal_queue import InternalQueue, ControlElement
from core.util import get_one_of, set_one_of
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    ReturnInvocation,
    ControlRequest,
    ControlInvocation,
    ControlReturn,
    ControlError,
    ErrorLanguage,
)
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    ControlPayloadV2,
)


class AsyncRPCServer:
    def __init__(self, output_queue: InternalQueue, context: Context):
        self._output_queue = output_queue
        rpc_mapping = AsyncRPCHandlerInitializer(context).__mapping__()
        self._handlers: dict[str, grpclib.const.Handler] = {
            k.path.split("/")[-1].lower(): v for k, v in rpc_mapping.items()
        }

    def wrap_as_stream(self, request: ControlRequest) -> grpclib.server.Stream:
        class ControlRequestStream(grpclib.server.Stream):
            async def recv_message(self):
                return request

        return ControlRequestStream()

    def receive(
        self, from_: ActorVirtualIdentity, control_invocation: ControlInvocation
    ):
        command: ControlRequest = get_one_of(control_invocation.command)
        method_name = control_invocation.method_name
        logger.debug(f"PYTHON receives a ControlInvocation: {control_invocation}")
        try:
            handler: grpclib.const.Handler = self.look_up(method_name.lower())
            control_payload_stream = self.wrap_as_stream(command)
            control_return: ControlReturn = set_one_of(
                ControlReturn, handler.func(control_payload_stream)
            )

        except Exception as exception:
            logger.exception(exception)
            control_return: ControlReturn = set_one_of(
                ControlReturn,
                ControlError(
                    error_message=str(exception), language=ErrorLanguage.PYTHON
                ),
            )

        payload: ControlPayloadV2 = set_one_of(
            ControlPayloadV2,
            ReturnInvocation(
                command_id=control_invocation.command_id,
                return_value=control_return,
            ),
        )

        if self._no_reply_needed(control_invocation.command_id):
            return

        # reply to the sender
        to = from_
        logger.debug(
            f"PYTHON returns a ReturnInvocation {payload}, replying the command"
            f" {command}"
        )
        self._output_queue.put(ControlElement(tag=to, payload=payload))

    def look_up(self, method_name: str) -> grpclib.const.Handler:
        logger.debug(method_name)
        return self._handlers[method_name]

    @staticmethod
    def _no_reply_needed(command_id: int) -> bool:
        return command_id < 0
