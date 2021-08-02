from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context

from proto.edu.uci.ics.amber.engine.architecture.worker import PauseWorkerV2, WorkerState


class PauseWorkerHandler(Handler):
    cmd = PauseWorkerV2

    def __call__(self, context: Context, command: PauseWorkerV2, *args, **kwargs):
        if context.state_manager.confirm_state(WorkerState.RUNNING, WorkerState.READY):
            context.pause_manager.pause()
            context.input_queue.disable_sub()
            context.state_manager.transit_to(WorkerState.PAUSED)
        state = context.state_manager.get_current_state()
        return state
