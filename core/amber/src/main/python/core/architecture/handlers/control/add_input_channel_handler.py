from core.architecture.handlers.control.control_handler_base import ControlHandler
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    AddInputChannelRequest,
)


class AddInputChannelHandler(ControlHandler):
    async def add_input_channel(self, req: AddInputChannelRequest) -> EmptyReturn:
        if not req.channel_id.is_control:
            # Explicitly set is_control to trigger lazy computation.
            # If not set, it may be computed at different times,
            # causing hash inconsistencies.
            req.channel_id.is_control = False
        self.context.input_manager.register_input(req.channel_id, req.port_id)
        return EmptyReturn()
