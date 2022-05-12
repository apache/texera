from proto.edu.uci.ics.amber.engine.architecture.worker import (
    QueryStatisticsV2,
    WorkerStatistics,
)
from .handler_base import Handler
from ..managers.context import Context


class QueryStatisticsHandler(Handler):
    cmd = QueryStatisticsV2

    def __call__(self, context: Context, command: QueryStatisticsV2, *args, **kwargs):
        input_count, output_count = context.statistics_manager.get_statistics()
        state = context.state_manager.get_current_state()
        return WorkerStatistics(
            worker_state=state,
            input_tuple_count=input_count,
            output_tuple_count=output_count,
        )
