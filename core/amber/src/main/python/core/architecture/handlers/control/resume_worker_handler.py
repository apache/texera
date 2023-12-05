from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context
from core.architecture.managers.pause_manager import PauseType

from proto.edu.uci.ics.amber.engine.architecture.worker import ResumeWorkerV2


class ResumeWorkerHandler(ControlHandler):
    cmd = ResumeWorkerV2

    def __call__(self, context: Context, command: ResumeWorkerV2, *args, **kwargs):
        context.pause_manager.resume(PauseType.USER_PAUSE)
        state = context.state_manager.get_current_state()
        return state
