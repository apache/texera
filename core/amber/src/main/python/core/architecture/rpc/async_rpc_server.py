from loguru import logger

from core.architecture.handlers.control.add_partitioning_handler import (
    AddPartitioningHandler,
)
from core.architecture.handlers.control.assign_port_handler import AssignPortHandler
from core.architecture.handlers.control.debug_command_handler import (
    WorkerDebugCommandHandler,
)
from core.architecture.handlers.control.evaluate_expression_handler import (
    EvaluateExpressionHandler,
)
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.handlers.control.initialize_operator_logic_handler import (
    InitializeOperatorLogicHandler,
)
from core.architecture.handlers.control.modify_operator_logic_handler import (
    ModifyOperatorLogicHandler,
)
from core.architecture.handlers.control.no_op_handler import NoOpHandler
from core.architecture.handlers.control.open_operator_handler import OpenOperatorHandler
from core.architecture.handlers.control.pause_worker_handler import PauseWorkerHandler
from core.architecture.handlers.control.query_current_input_tuple_handler import (
    QueryCurrentInputTupleHandler,
)
from core.architecture.handlers.control.query_statistics_handler import (
    QueryStatisticsHandler,
)
from core.architecture.handlers.control.replay_current_tuple_handler import (
    ReplayCurrentTupleHandler,
)
from core.architecture.handlers.control.resume_worker_handler import ResumeWorkerHandler
from core.architecture.handlers.control.start_worker_handler import StartWorkerHandler
from core.architecture.handlers.control.add_input_channel_handler import (
    AddInputChannelHandler,
)
from core.architecture.handlers.control.scheduler_time_slot_event_handler import (
    SchedulerTimeSlotEventHandler,
)
from core.architecture.managers.context import Context
from core.models.internal_queue import InternalQueue, ControlElement
from core.util import get_one_of, set_one_of
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    ControlCommandV2,
    ControlException,
    ControlReturnV2,
)
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    ControlInvocationV2,
    ControlPayloadV2,
    ReturnInvocationV2,
)


class AsyncRPCServer:
    def __init__(self, output_queue: InternalQueue, context: Context):
        self._context = context
        self._output_queue = output_queue
        self._handlers: dict[type(ControlCommandV2), ControlHandler] = dict()
        self.register(NoOpHandler())
        self.register(StartWorkerHandler())
        self.register(PauseWorkerHandler())
        self.register(ResumeWorkerHandler())
        self.register(OpenOperatorHandler())
        self.register(AssignPortHandler())
        self.register(AddPartitioningHandler())
        self.register(AddInputChannelHandler())
        self.register(QueryStatisticsHandler())
        self.register(QueryCurrentInputTupleHandler())
        self.register(InitializeOperatorLogicHandler())
        self.register(ModifyOperatorLogicHandler())
        self.register(ReplayCurrentTupleHandler())
        self.register(EvaluateExpressionHandler())
        self.register(SchedulerTimeSlotEventHandler())
        self.register(WorkerDebugCommandHandler())

    def receive(
        self, from_: ActorVirtualIdentity, control_invocation: ControlInvocationV2
    ):
        command: ControlCommandV2 = get_one_of(control_invocation.command)
        logger.debug(f"PYTHON receives a ControlInvocation: {control_invocation}")
        try:
            handler = self.look_up(command)
            control_return: ControlReturnV2 = set_one_of(
                ControlReturnV2, handler(self._context, command)
            )

        except Exception as exception:
            logger.exception(exception)
            control_return: ControlReturnV2 = set_one_of(
                ControlReturnV2, ControlException(str(exception))
            )

        payload: ControlPayloadV2 = set_one_of(
            ControlPayloadV2,
            ReturnInvocationV2(
                original_command_id=control_invocation.command_id,
                control_return=control_return,
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

    def register(self, handler: ControlHandler) -> None:
        self._handlers[handler.cmd] = handler

    def look_up(self, cmd: ControlCommandV2) -> ControlHandler:
        logger.debug(cmd)
        return self._handlers[type(cmd)]

    @staticmethod
    def _no_reply_needed(command_id: int) -> bool:
        return command_id < 0
