from proto.edu.uci.ics.amber.engine.architecture.worker import InitializeOperatorLogicV2
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context


class InitializeOperatorLogicHandler(ControlHandler):
    cmd = InitializeOperatorLogicV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        context.operator_manager.initialize_operator(
            command.code, command.is_source, command.output_schema
        )
        return None
