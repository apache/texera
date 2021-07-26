from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context

from proto.edu.uci.ics.amber.engine.architecture.worker import ResumeWorkerV2, WorkerState


class ResumeWorkerHandler(Handler):
    cmd = ResumeWorkerV2

    def __call__(self, context: Context, command: ResumeWorkerV2, *args, **kwargs):
        if context.state_manager.confirm_state(WorkerState.PAUSED):
            if context.pause_manager.is_paused():
                context.pause_manager.resume()
                context.dp._input_queue.enable_sub()
            context.state_manager.transit_to(WorkerState.RUNNING)
        state = context.state_manager.get_current_state()
        return state
