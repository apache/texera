from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context
from core.architecture.managers.pause_manager import PauseType

from proto.edu.uci.ics.amber.engine.architecture.worker import PauseWorkerV2


class PauseWorkerHandler(Handler):
    cmd = PauseWorkerV2

    def __call__(self, context: Context, command: PauseWorkerV2, *args, **kwargs):
        context.pause_manager.pause(PauseType.USER_PAUSE)
        state = context.state_manager.get_current_state()
        return state
