from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context
from proto.edu.uci.ics.amber.engine.architecture.worker import AssignPortV2


class AssignPortHandler(ControlHandler):
    cmd = AssignPortV2

    def __call__(self, context: Context, command: AssignPortV2, *args, **kwargs):
        context.batch_to_tuple_converter.add_input_port(command.port_id)
        return None
