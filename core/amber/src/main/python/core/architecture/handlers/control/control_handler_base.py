from abc import ABC

from core.architecture.managers.context import Context
from proto.edu.uci.ics.amber.engine.architecture.rpc import ControlRequest
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    ControlCommandV2,
    ControlReturnV2,
)


class ControlHandler(ABC):
    method_name: str = None
    req: ControlRequest = None

    def __call__(
        self, context: Context, command: ControlCommandV2, *args, **kwargs
    ) -> ControlReturnV2:
        pass
