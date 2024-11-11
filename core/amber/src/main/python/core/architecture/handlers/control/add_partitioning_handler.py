from core.architecture.handlers.control.control_handler_base import ControlHandler
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    AddPartitioningRequest,
)


class AddPartitioningHandler(ControlHandler):

    async def add_partitioning(self, req: AddPartitioningRequest) -> EmptyReturn:
        self.context.output_manager.add_partitioning(req.tag, req.partitioning)
        return EmptyReturn()
