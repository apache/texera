from core.architecture.handlers.control.control_handler_base import ControlHandler
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    InitializeExecutorRequest,
)


class InitializeExecutorHandler(ControlHandler):

    async def initialize_executor(self, req: InitializeExecutorRequest) -> EmptyReturn:
        code = req.op_exec_init_info.value.decode("utf-8")
        self.context.executor_manager.initialize_executor(
            code, req.is_source, req.language
        )
        return EmptyReturn()
