from proto.edu.uci.ics.amber.engine.architecture.worker import NoOpV2
from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context


class NoOpHandler(Handler):
    cmd = NoOpV2

    def __call__(self, context: Context, command: NoOpV2, *args, **kwargs):
        # do nothing
        return None
