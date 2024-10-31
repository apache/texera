from core.architecture.handlers.control.control_handler_base import ControlHandler
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    EmptyReturn,
    EmptyRequest,
)


class NoOperationHandler(ControlHandler):

    async def no_operation(self, req: EmptyRequest) -> EmptyReturn:
        return EmptyReturn()
