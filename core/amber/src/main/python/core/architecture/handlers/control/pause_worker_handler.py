from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.pause_manager import PauseType
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    WorkerStateResponse,
    EmptyRequest,
)


class PauseWorkerHandler(ControlHandler):

    async def pause_worker(self, req: EmptyRequest) -> WorkerStateResponse:
        self.context.pause_manager.pause(PauseType.USER_PAUSE)
        state = self.context.state_manager.get_current_state()
        return WorkerStateResponse(state)
