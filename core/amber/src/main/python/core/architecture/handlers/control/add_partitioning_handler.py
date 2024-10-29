from core.architecture.handlers.control.control_handler_base import ControlHandler
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    AddPartitioningRequest,
)
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import Partitioning
from proto.edu.uci.ics.amber.engine.common import PhysicalLink


class AddPartitioningHandler(ControlHandler):

    async def add_partitioning(self, req: AddPartitioningRequest) -> EmptyReturn:
        self.context.output_manager.add_partitioning(req.tag, req.partitioning)
        return EmptyReturn()
