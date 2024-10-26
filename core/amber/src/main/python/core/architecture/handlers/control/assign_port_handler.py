from typing import Dict

from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.models import Schema
from proto.edu.uci.ics.amber.engine.architecture.rpc import EmptyReturn
from proto.edu.uci.ics.amber.engine.common import PortIdentity


class AssignPortHandler(ControlHandler):

    def assign_port(
        self, port_id: PortIdentity, input: bool, schema: Dict[str, str]
    ) -> EmptyReturn:
        if input:
            self.context.input_manager.add_input_port(
                port_id, Schema(raw_schema=schema)
            )
        else:
            self.context.output_manager.add_output_port(
                port_id, Schema(raw_schema=schema)
            )
        return EmptyReturn()
