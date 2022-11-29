import itertools

from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    ReplayCurrentTupleV2,
    WorkerState,
)


class ReplayCurrentTupleHandler(Handler):
    cmd = ReplayCurrentTupleV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        if not context.state_manager.confirm_state(WorkerState.COMPLETED):
            # chain the current input tuple back on top of the current iterator to
            # be processed once more
            context.tuple_processing_manager.current_input_tuple_iter = itertools.chain(
                [context.tuple_processing_manager.current_input_tuple],
                context.tuple_processing_manager.current_input_tuple_iter,
            )
        return None
