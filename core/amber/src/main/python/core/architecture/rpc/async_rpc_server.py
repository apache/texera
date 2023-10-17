from loguru import logger

from core.architecture.handlers.add_partitioning_handler import AddPartitioningHandler
from core.architecture.handlers.debug_command_handler import WorkerDebugCommandHandler
from core.architecture.handlers.evaluate_expression_handler import (
    EvaluateExpressionHandler,
)
from core.architecture.handlers.handler_base import Handler
from core.architecture.handlers.initialize_operator_logic_handler import (
    InitializeOperatorLogicHandler,
)
from core.architecture.handlers.modify_operator_logic_handler import (
    ModifyOperatorLogicHandler,
)
from core.architecture.handlers.monitoring_handler import MonitoringHandler
from core.architecture.handlers.open_operator_handler import OpenOperatorHandler
from core.architecture.handlers.pause_worker_handler import PauseWorkerHandler
from core.architecture.handlers.backpressure_handler import BackpressureHandler
from core.architecture.handlers.query_current_input_tuple_handler import (
    QueryCurrentInputTupleHandler,
)
from core.architecture.handlers.query_statistics_handler import QueryStatisticsHandler
from core.architecture.handlers.replay_current_tuple_handler import (
    ReplayCurrentTupleHandler,
)
from core.architecture.handlers.resume_worker_handler import ResumeWorkerHandler
from core.architecture.handlers.start_worker_handler import StartWorkerHandler
from core.architecture.handlers.update_input_linking_handler import (
    UpdateInputLinkingHandler,
)
from core.architecture.handlers.scheduler_time_slot_event_handler import (
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
        self._handlers: dict[type(ControlCommandV2), Handler] = dict()
        self.register(StartWorkerHandler())
        self.register(PauseWorkerHandler())
        self.register(BackpressureHandler())
        self.register(ResumeWorkerHandler())
        self.register(OpenOperatorHandler())
        self.register(AddPartitioningHandler())
        self.register(UpdateInputLinkingHandler())
        self.register(QueryStatisticsHandler())
        self.register(QueryCurrentInputTupleHandler())
        self.register(InitializeOperatorLogicHandler())
        self.register(ModifyOperatorLogicHandler())
        self.register(ReplayCurrentTupleHandler())
        self.register(EvaluateExpressionHandler())
        self.register(MonitoringHandler())
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

        # reply to the sender
        to = from_
        logger.debug(
            f"PYTHON returns a ReturnInvocation {payload}, replying the command"
            f" {command}"
        )
        self._output_queue.put(ControlElement(tag=to, payload=payload))

    def register(self, handler: Handler) -> None:
        self._handlers[handler.cmd] = handler

    def look_up(self, cmd: ControlCommandV2) -> Handler:
        logger.debug(cmd)
        return self._handlers[type(cmd)]
