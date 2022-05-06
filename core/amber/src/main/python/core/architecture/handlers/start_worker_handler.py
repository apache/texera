from proto.edu.uci.ics.amber.engine.architecture.worker import (
    StartWorkerV2,
    WorkerState,
)
from .handler_base import Handler
from ..managers.context import Context
from ..packaging.batch_to_tuple_converter import BatchToTupleConverter
from ...models import DataElement


class StartWorkerHandler(Handler):
    cmd = StartWorkerV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        if context.dp._operator.is_source:
            context.state_manager.transit_to(WorkerState.RUNNING)
            context.input_queue.put(
                DataElement(tag=BatchToTupleConverter.SOURCE_STARTER, payload=None)
            )
        state = context.state_manager.get_current_state()
        return state
