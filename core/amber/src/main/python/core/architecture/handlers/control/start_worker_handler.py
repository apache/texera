from proto.edu.uci.ics.amber.engine.architecture.worker import (
    StartWorkerV2,
    WorkerState,
)
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context
from core.architecture.packaging.input_manager import InputManager
from core.models.internal_queue import DataElement

from loguru import logger


class StartWorkerHandler(ControlHandler):
    cmd = StartWorkerV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        logger.info("Starting the worker.")
        if context.executor_manager.executor.is_source:
            context.state_manager.transit_to(WorkerState.RUNNING)
            context.input_queue.put(
                DataElement(tag=InputManager.SOURCE_STARTER, payload=None)
            )
        state = context.state_manager.get_current_state()
        return state
