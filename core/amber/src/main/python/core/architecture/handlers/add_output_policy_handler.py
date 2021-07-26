from proto.edu.uci.ics.amber.engine.architecture.worker import AddPartitioningV2
from .handler_base import Handler
from ..managers.context import Context


class AddOutputPolicyHandler(Handler):
    cmd = AddPartitioningV2

    def __call__(self, context: Context, command: AddPartitioningV2, *args, **kwargs):
        context.tuple_to_batch_converter.add_policy(command.partitioning)
        return None
