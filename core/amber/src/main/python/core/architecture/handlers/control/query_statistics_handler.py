from proto.edu.uci.ics.amber.engine.architecture.worker import (
    QueryStatisticsV2,
    WorkerStatistics,
)
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context


class QueryStatisticsHandler(ControlHandler):
    cmd = QueryStatisticsV2

    def __call__(self, context: Context, command: QueryStatisticsV2, *args, **kwargs):
        return context.state_manager.get_current_state(), WorkerStatistics(*context.statistics_manager.get_statistics())
