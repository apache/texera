from betterproto.lib.google.protobuf import Any

from core.architecture.handlers.control.control_handler_base import ControlHandler
from proto.edu.uci.ics.amber.engine.architecture.rpc import EmptyReturn


class InitializeExecutorHandler(ControlHandler):

    async def initialize_executor(
        self,
        total_worker_count: int,
        op_exec_init_info: Any,
        is_source: bool,
        language: str,
    ) -> EmptyReturn:
        code = op_exec_init_info.value.decode("utf-8")
        self.context.executor_manager.initialize_executor(code, is_source, language)
        return EmptyReturn()
