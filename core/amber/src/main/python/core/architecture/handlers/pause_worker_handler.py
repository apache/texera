from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context

from proto.edu.uci.ics.amber.engine.architecture.worker import PauseWorkerV2


class PauseWorkerHandler(Handler):
    cmd = PauseWorkerV2

    def __call__(self, context: Context, command: PauseWorkerV2, *args, **kwargs):
        context.dp._pause()
        state = context.state_manager.get_current_state()
        return state
