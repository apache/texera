from proto.edu.uci.ics.amber.engine.architecture.worker import OpenOperatorV2
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context


class OpenOperatorHandler(ControlHandler):
    cmd = OpenOperatorV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        context.operator_manager.operator.open()
        return None
