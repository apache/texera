from core.architecture.handlers.control.control_handler_base import ControlHandler
from proto.edu.uci.ics.amber.engine.architecture.rpc import EmptyReturn, EmptyRequest


class OpenExecutorHandler(ControlHandler):

    async def open_executor(self, req: EmptyRequest) -> EmptyReturn:
        self.context.executor_manager.executor.open()
        return EmptyReturn()
