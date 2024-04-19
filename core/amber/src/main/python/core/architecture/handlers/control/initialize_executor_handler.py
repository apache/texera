from proto.edu.uci.ics.amber.engine.architecture.worker import InitializeExecutorV2
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context


class InitializeExecutorHandler(ControlHandler):
    cmd = InitializeExecutorV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        if command.language == "r":
            context.executor_manager.initialize_r_executor(
                command.code, command.is_source
            )
        else:

            context.executor_manager.initialize_executor(
                command.code, command.is_source
            )

        return None
