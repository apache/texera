from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.util import get_one_of
from proto.edu.uci.ics.amber.core import OpExecWithCode
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    InitializeExecutorRequest,
)


class InitializeExecutorHandler(ControlHandler):

    async def initialize_executor(self, req: InitializeExecutorRequest) -> EmptyReturn:
        op_exec_with_code: OpExecWithCode = get_one_of(req.op_exec_init_info)
        self.context.executor_manager.initialize_executor(
            op_exec_with_code.code, req.is_source, op_exec_with_code.language
        )
        return EmptyReturn()
