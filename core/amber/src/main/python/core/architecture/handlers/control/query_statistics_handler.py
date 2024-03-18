from proto.edu.uci.ics.amber.engine.architecture.worker import (
    QueryStatisticsV2,
    WorkerStatistics,
)
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context


class QueryStatisticsHandler(ControlHandler):
    cmd = QueryStatisticsV2

    def __call__(self, context: Context, command: QueryStatisticsV2, *args, **kwargs):
        stats = context.statistics_manager.get_statistics()
        return WorkerStatistics(
            worker_state=context.state_manager.get_current_state(),
            input_tuple_count=stats[0],
            output_tuple_count=stats[1],
            data_processing_time=stats[2],
            control_processing_time=stats[3],
            idle_time=stats[4],
        )
