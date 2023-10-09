from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context

from proto.edu.uci.ics.amber.engine.architecture.worker import BackpressureV2


class BackpressureHandler(Handler):
    cmd = BackpressureV2

    def __call__(self, context: Context, command: BackpressureV2, *args, **kwargs):
        print("I receive:", command)
        if command.enable_backpressure:
            context.main_loop._pause_dp()
        else:
            context.main_loop._resume_dp()
