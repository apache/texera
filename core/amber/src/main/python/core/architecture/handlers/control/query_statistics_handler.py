from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    WorkerMetricsResponse,
    EmptyRequest,
)
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    WorkerMetrics,
)
from core.architecture.handlers.control.control_handler_base import ControlHandler


class QueryStatisticsHandler(ControlHandler):

    async def query_statistics(self, req: EmptyRequest) -> WorkerMetricsResponse:
        metrics = WorkerMetrics(
            worker_state=self.context.state_manager.get_current_state(),
            worker_statistics=self.context.statistics_manager.get_statistics(),
        )
        return WorkerMetricsResponse(metrics)
