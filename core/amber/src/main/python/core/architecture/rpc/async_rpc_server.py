from core.architecture.handlers.add_partitioning_handler import AddOutputPolicyHandler
from core.architecture.handlers.handler_base import Handler
from core.architecture.handlers.pause_worker_handler import PauseWorkerHandler
from core.architecture.handlers.query_statistics_handler import QueryStatisticsHandler
from core.architecture.handlers.resume_worker_handler import ResumeWorkerHandler

from core.architecture.handlers.start_worker_handler import StartWorkerHandler
from core.architecture.handlers.update_input_linking_handler import UpdateInputLinkingHandler
from core.architecture.managers.context import Context
from core.models.internal_queue import ControlElement, InternalQueue
from core.util import get_one_of, set_one_of
from proto.edu.uci.ics.amber.engine.architecture.worker import ControlCommandV2, ControlReturnV2
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlInvocationV2, ControlPayloadV2, \
    ReturnInvocationV2


class AsyncRPCServer:
    def __init__(self, output_queue: InternalQueue, context: Context):
        self._context = context
        self._output_queue = output_queue
        self._handlers: dict[type(ControlCommandV2), Handler] = dict()
        self.register(StartWorkerHandler())
        self.register(PauseWorkerHandler())
        self.register(ResumeWorkerHandler())
        self.register(AddOutputPolicyHandler())
        self.register(UpdateInputLinkingHandler())
        self.register(QueryStatisticsHandler())

    def receive(self, from_: ActorVirtualIdentity, control_invocation: ControlInvocationV2):
        command: ControlCommandV2 = get_one_of(control_invocation.command)
        # logger.info(f"PYTHON receive a CONTROL: {control_invocation}")
        handler = self.look_up(command)
        control_return: ControlReturnV2 = set_one_of(ControlReturnV2, handler(self._context, command))

        payload: ControlPayloadV2 = set_one_of(
            ControlPayloadV2,
            ReturnInvocationV2(
                original_command_id=control_invocation.command_id,
                control_return=control_return
            )
        )

        # reply to the sender
        to = from_
        # logger.info(f"PYTHON returning control {cmd}")
        self._output_queue.put(ControlElement(tag=to, payload=payload))

    def register(self, handler: Handler) -> None:
        self._handlers[handler.cmd] = handler

    def look_up(self, cmd: ControlCommandV2) -> Handler:
        return self._handlers[type(cmd)]
