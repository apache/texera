from core.architecture.handlers.control.control_handler_base import ControlHandler
from proto.edu.uci.ics.amber.engine.architecture.rpc import EmptyReturn


class OpenExecutorHandler(ControlHandler):

    def open_executor(self) -> EmptyReturn:
        self.context.executor_manager.executor.open()
        return EmptyReturn()
