import itertools

from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context
from proto.edu.uci.ics.amber.engine.architecture.worker import RetryPythonV2


class RetryPythonHandler(Handler):
    cmd = RetryPythonV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        # chain the current input tuple back on top of the current iterator to
        # be processed once more
        context.dp._current_input_tuple_iter = itertools.chain(
            [context.dp._current_input_tuple],
            context.dp._current_input_tuple_iter
        )
        context.dp._resume()
        state = context.state_manager.get_current_state()
        return state
