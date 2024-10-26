from core.architecture.handlers.control.control_handler_base import ControlHandler
from proto.edu.uci.ics.amber.engine.architecture.rpc import EmptyReturn
from proto.edu.uci.ics.amber.engine.architecture.sendsemantics import Partitioning
from proto.edu.uci.ics.amber.engine.common import PhysicalLink


class AddPartitioningHandler(ControlHandler):

    def add_partitioning(
        self,
        tag: PhysicalLink,
        partitioning: Partitioning,
    ) -> EmptyReturn:
        self.context.output_manager.add_partitioning(tag, partitioning)
        return EmptyReturn()
