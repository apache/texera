from proto.edu.uci.ics.amber.engine.architecture.worker import QuerySelfWorkloadMetricsV2, SelfWorkloadMetrics
from .handler_base import Handler
from ..managers.context import Context


class MonitoringHandler(Handler):
    cmd = QuerySelfWorkloadMetricsV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        try:
            return SelfWorkloadMetrics(
                context.input_queue.sub_size(),
                context.input_queue.main_size(),
                # TODO: Given that following information are retained at java side which is
                #   hard to fetch, and the fact that those two information are not
                #   urgently used, we decided to hard code them to 0 for now.
                #   Later if we have the OrderingEnforcer is ported to Python side, we can
                #   collect those information.
                0,  # dataInputPort.getStashedMessageCount()
                0   # controlInputPort.getStashedMessageCount()

            )
        except:
            return SelfWorkloadMetrics(-1, -1, -1, -1)
