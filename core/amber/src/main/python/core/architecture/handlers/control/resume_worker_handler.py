from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context
from core.architecture.managers.pause_manager import PauseType
from proto.edu.uci.ics.amber.engine.architecture.rpc import WorkerStateResponse


class ResumeWorkerHandler(ControlHandler):

    def resume_worker(self) -> WorkerStateResponse:
        self.context.pause_manager.resume(PauseType.USER_PAUSE)
        state = self.context.state_manager.get_current_state()
        return WorkerStateResponse(state)
