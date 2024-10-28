from core.architecture.handlers.control.control_handler_base import ControlHandler
from proto.edu.uci.ics.amber.engine.architecture.rpc import EmptyReturn
from proto.edu.uci.ics.amber.engine.common import ChannelIdentity, PortIdentity


class AddInputChannelHandler(ControlHandler):
    async def add_input_channel(
        self,
        channel_id: ChannelIdentity,
        port_id: PortIdentity,
    ) -> EmptyReturn:
        self.context.input_manager.register_input(channel_id, port_id)
        return EmptyReturn()
