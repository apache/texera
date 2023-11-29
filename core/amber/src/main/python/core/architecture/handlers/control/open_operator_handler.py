from proto.edu.uci.ics.amber.engine.architecture.worker import OpenOperatorV2
from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context


class OpenOperatorHandler(Handler):
    cmd = OpenOperatorV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        context.operator_manager.operator.open()
        return None
