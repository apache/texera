from core.architecture.handlers.control.control_handler_base import ControlHandler
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    AddInputChannelRequest,
)


class AddInputChannelHandler(ControlHandler):
    async def add_input_channel(self, req: AddInputChannelRequest) -> EmptyReturn:
        self.context.input_manager.register_input(req.channel_id, req.port_id)
        return EmptyReturn()
