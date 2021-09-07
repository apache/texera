from loguru import logger

from pyamber.architecture.handlers.add_partitioning_handler import AddPartitioningHandler
from pyamber.architecture.handlers.handler_base import Handler
from pyamber.architecture.handlers.pause_worker_handler import PauseWorkerHandler
from pyamber.architecture.handlers.query_current_input_tuple_handler import QueryCurrentInputTupleHandler
from pyamber.architecture.handlers.query_statistics_handler import QueryStatisticsHandler
from pyamber.architecture.handlers.resume_worker_handler import ResumeWorkerHandler
from pyamber.architecture.handlers.send_python_udf_handler import SendPythonUdfHandler
from pyamber.architecture.handlers.start_worker_handler import StartWorkerHandler
from pyamber.architecture.handlers.update_input_linking_handler import UpdateInputLinkingHandler
from pyamber.architecture.managers.context import Context
from pyamber.models.internal_queue import ControlElement, InternalQueue
from pyamber.util import get_one_of, set_one_of
from proto.edu.uci.ics.amber.engine.architecture.worker import ControlCommandV2, ControlException, ControlReturnV2
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
        self.register(AddPartitioningHandler())
        self.register(UpdateInputLinkingHandler())
        self.register(QueryStatisticsHandler())
        self.register(QueryCurrentInputTupleHandler())
        self.register(SendPythonUdfHandler())

    def receive(self, from_: ActorVirtualIdentity, control_invocation: ControlInvocationV2):
        command: ControlCommandV2 = get_one_of(control_invocation.command)
        logger.debug(f"PYTHON receives a ControlInvocation: {control_invocation}")
        try:
            handler = self.look_up(command)
            control_return: ControlReturnV2 = set_one_of(ControlReturnV2, handler(self._context, command))

        except Exception as exception:
            control_return: ControlReturnV2 = set_one_of(ControlReturnV2, ControlException(str(exception)))

        payload: ControlPayloadV2 = set_one_of(
            ControlPayloadV2,
            ReturnInvocationV2(
                original_command_id=control_invocation.command_id,
                control_return=control_return
            )
        )

        # reply to the sender
        to = from_
        logger.debug(f"PYTHON returns a ReturnInvocation {payload}, replying the command {command}")
        self._output_queue.put(ControlElement(tag=to, payload=payload))

    def register(self, handler: Handler) -> None:
        self._handlers[handler.cmd] = handler

    def look_up(self, cmd: ControlCommandV2) -> Handler:
        logger.debug(cmd)
        return self._handlers[type(cmd)]
