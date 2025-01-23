import grpclib.const
from loguru import logger
import asyncio
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
from proto.edu.uci.ics.amber.engine.common import ControlPayloadV2
from proto.edu.uci.ics.amber.core import ActorVirtualIdentity


class AsyncRPCServer:
    def __init__(self, output_queue: InternalQueue, context: Context):
        self._output_queue = output_queue
        rpc_mapping = AsyncRPCHandlerInitializer(context).__mapping__()
        self._handlers: dict[str, grpclib.const.Handler] = {
            k.split("/")[-1].lower(): v for k, v in rpc_mapping.items()
        }

    def _wrap_as_stream(self, request: ControlRequest) -> grpclib.server.Stream:
        """
        Wraps a ControlRequest as a grpclib server Stream.

        :param request: The ControlRequest to be wrapped.
        :return: A Stream object that provides asynchronous send and receive methods.

        This allows the incoming ControlRequest to be treated as a streaming request
        for compatibility with grpclib's handler interface.
        """

        class ControlRequestStream(grpclib.server.Stream):
            def __init__(self):
                self.result = None

            async def recv_message(self):
                return request

            async def send_message(self, msg):
                self.result = msg

        return ControlRequestStream()

    def receive(
        self, from_: ActorVirtualIdentity, control_invocation: ControlInvocation
    ):
        """
        Handles incoming ControlInvocation messages by invoking the appropriate handler.

        :param from_: The sender's ActorVirtualIdentity.
        :param control_invocation: The incoming ControlInvocation message.

        This method performs the following steps:
        1. Extracts the command from the ControlInvocation.
        2. Looks up the corresponding handler for the method name.
        3. Wraps the command as a stream and runs the handler asynchronously.
        4. Constructs a ControlReturn or ControlError based on the handler's result.
        5. Sends the response back to the sender, unless no reply is needed.
        """
        command: ControlRequest = get_one_of(control_invocation.command)
        method_name = control_invocation.method_name
        logger.debug(f"PYTHON receives a ControlInvocation: {control_invocation}")
        try:
            # Look up the handler based on the lowercase method name.
            handler: grpclib.const.Handler = self.look_up(method_name.lower())
            # Wrap the command as a streaming request.
            control_payload_stream = self._wrap_as_stream(command)
            # Run the handler asynchronously.
            asyncio.run(handler.func(control_payload_stream))
            # Set up a ControlReturn from the handler's result.
            control_return: ControlReturn = set_one_of(
                ControlReturn, control_payload_stream.result
            )

        except Exception as exception:
            # Handle exceptions and log the error.
            logger.exception(exception)
            # Construct a ControlError message in case of an exception.
            control_return: ControlReturn = set_one_of(
                ControlReturn,
                ControlError(
                    error_message=str(exception), language=ErrorLanguage.PYTHON
                ),
            )

        # Construct the payload as a ReturnInvocation.
        payload: ControlPayloadV2 = set_one_of(
            ControlPayloadV2,
            ReturnInvocation(
                command_id=control_invocation.command_id,
                return_value=control_return,
            ),
        )

        # Check if a reply is needed; if not, return early.
        if self._no_reply_needed(control_invocation.command_id):
            return

        # Reply to the sender.
        to = from_
        logger.debug(
            f"PYTHON returns a ReturnInvocation {payload}, replying the command"
            f" {command}"
        )
        # Put the control element in the output queue.
        self._output_queue.put(ControlElement(tag=to, payload=payload))

    def look_up(self, method_name: str) -> grpclib.const.Handler:
        logger.debug(method_name)
        return self._handlers[method_name]

    @staticmethod
    def _no_reply_needed(command_id: int) -> bool:
        return command_id < 0
