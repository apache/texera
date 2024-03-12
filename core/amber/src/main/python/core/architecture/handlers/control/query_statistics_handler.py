from proto.edu.uci.ics.amber.engine.architecture.worker import (
    QueryStatisticsV2,
    WorkerStatistics,
)
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context


class QueryStatisticsHandler(ControlHandler):
    cmd = QueryStatisticsV2

    def __call__(self, context: Context, command: QueryStatisticsV2, *args, **kwargs):
        (
            input_count,
            output_count,
            data_processing_time,
            control_processing_time,
            idle_time,
        ) = context.statistics_manager.get_statistics()
        state = context.state_manager.get_current_state()
        return WorkerStatistics(
            worker_state=state,
            input_tuple_count=input_count,
            output_tuple_count=output_count,
            data_processing_time=data_processing_time,
            control_processing_time=control_processing_time,
            idle_time=idle_time,
        )
