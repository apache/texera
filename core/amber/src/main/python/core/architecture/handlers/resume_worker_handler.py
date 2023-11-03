from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context

from proto.edu.uci.ics.amber.engine.architecture.worker import ResumeWorkerV2


class ResumeWorkerHandler(Handler):
    cmd = ResumeWorkerV2

    def __call__(self, context: Context, command: ResumeWorkerV2, *args, **kwargs):
        context.main_loop._resume_dp()
        state = context.state_manager.get_current_state()
        return state
