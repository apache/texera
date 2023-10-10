from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context
from core.architecture.managers.pause_manager import PauseType

from proto.edu.uci.ics.amber.engine.architecture.worker import BackpressureV2


class BackpressureHandler(Handler):
    cmd = BackpressureV2

    def __call__(self, context: Context, command: BackpressureV2, *args, **kwargs):
        if command.enable_backpressure:
            context.pause_manager.pause(PauseType.BACKPRESSURE_PAUSE)
        else:
            context.pause_manager.resume(PauseType.BACKPRESSURE_PAUSE)
