from proto.edu.uci.ics.amber.engine.architecture.worker import AddPartitioningV2
from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context


class AddPartitioningHandler(Handler):
    cmd = AddPartitioningV2

    def __call__(self, context: Context, command: AddPartitioningV2, *args, **kwargs):
        context.tuple_to_batch_converter.add_partitioning(
            command.tag, command.partitioning
        )
        return None
