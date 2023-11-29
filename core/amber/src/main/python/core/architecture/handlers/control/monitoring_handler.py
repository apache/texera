from proto.edu.uci.ics.amber.engine.architecture.worker import (
    QuerySelfWorkloadMetricsV2,
    SelfWorkloadMetrics,
    SelfWorkloadReturn,
)
from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context


class MonitoringHandler(Handler):
    cmd = QuerySelfWorkloadMetricsV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        return SelfWorkloadReturn(
            SelfWorkloadMetrics(
                context.input_queue.size_data(),
                context.input_queue.size_control(),
                -1,  # TODO: dataInputPort.getStashedMessageCount()
                -1,  # TODO: controlInputPort.getStashedMessageCount()
            ),
            [],
        )
