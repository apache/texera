import itertools

from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.pause_manager import PauseType
from proto.edu.uci.ics.amber.engine.architecture.rpc import EmptyReturn, EmptyRequest
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    WorkerState,
)


class RetryCurrentTupleHandler(ControlHandler):

    async def retry_current_tuple(self, req: EmptyRequest) -> EmptyReturn:
        if not self.context.state_manager.confirm_state(WorkerState.COMPLETED):
            # chain the current input tuple back on top of the current iterator to
            # be processed once more
            self.context.tuple_processing_manager.current_input_tuple_iter = (
                itertools.chain(
                    [self.context.tuple_processing_manager.current_input_tuple],
                    self.context.tuple_processing_manager.current_input_tuple_iter,
                )
            )
            self.context.pause_manager.resume(PauseType.USER_PAUSE)
            self.context.pause_manager.resume(PauseType.EXCEPTION_PAUSE)
        return EmptyReturn()
