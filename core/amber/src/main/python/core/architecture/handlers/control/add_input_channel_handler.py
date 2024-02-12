from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context
from proto.edu.uci.ics.amber.engine.architecture.worker import AddInputChannelV2


class AddInputChannelHandler(ControlHandler):
    cmd = AddInputChannelV2

    def __call__(self, context: Context, command: AddInputChannelV2, *args, **kwargs):
        context.batch_to_tuple_converter.register_input(
            command.channel_id, command.port_id
        )
        return None
