from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context

from proto.edu.uci.ics.amber.engine.architecture.worker import RetryPythonV2


class RetryPythonHandler(Handler):
    cmd = RetryPythonV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        context.dp._resume(retry=True)
        state = context.state_manager.get_current_state()
        return state
