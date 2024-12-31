from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.util import get_one_of
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    InitializeExecutorRequest,
)


class InitializeExecutorHandler(ControlHandler):

    async def initialize_executor(self, req: InitializeExecutorRequest) -> EmptyReturn:
        code = get_one_of(req.op_exec_init_info).code
        self.context.executor_manager.initialize_executor(
            code, req.is_source, req.language
        )
        return EmptyReturn()
