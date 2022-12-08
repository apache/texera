from loguru import logger

from proto.edu.uci.ics.amber.engine.architecture.worker import (
    WorkerDebugCommandV2,
)
from .handler_base import Handler
from ..managers.context import Context


class WorkerDebugCommandHandler(Handler):
    cmd = WorkerDebugCommandV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        logger.info(f"Got WorkerDebugCommand {command}")
        return None
