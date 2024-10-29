from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.models import Schema
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    AssignPortRequest,
)


class AssignPortHandler(ControlHandler):

    async def assign_port(self, req: AssignPortRequest) -> EmptyReturn:
        if req.input:
            self.context.input_manager.add_input_port(
                req.port_id, Schema(raw_schema=req.schema)
            )
        else:
            self.context.output_manager.add_output_port(
                req.port_id, Schema(raw_schema=req.schema)
            )
        return EmptyReturn()
