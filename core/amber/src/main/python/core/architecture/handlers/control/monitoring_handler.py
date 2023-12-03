from proto.edu.uci.ics.amber.engine.architecture.worker import (
    QuerySelfWorkloadMetricsV2,
    SelfWorkloadMetrics,
    SelfWorkloadReturn,
)
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context


class MonitoringHandler(ControlHandler):
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
