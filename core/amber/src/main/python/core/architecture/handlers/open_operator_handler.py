from proto.edu.uci.ics.amber.engine.architecture.worker import OpenOperatorV2
from .handler_base import Handler
from ..managers.context import Context


class OpenOperatorHandler(Handler):
    cmd = OpenOperatorV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        context.operator_manager.operator.open()
        return None
