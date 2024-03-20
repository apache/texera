from proto.edu.uci.ics.amber.engine.architecture.worker import OpenExecutorV2
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context


class OpenExecutorHandler(ControlHandler):
    cmd = OpenExecutorV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        context.executor_manager.executor.open()
        return None
