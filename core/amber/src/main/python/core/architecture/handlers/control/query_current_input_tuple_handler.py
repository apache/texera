from proto.edu.uci.ics.amber.engine.architecture.worker import (
    CurrentInputTupleInfo,
    QueryCurrentInputTupleV2,
)
from core.architecture.handlers.control.control_handler_base import ControlHandler
from core.architecture.managers.context import Context


class QueryCurrentInputTupleHandler(ControlHandler):
    cmd = QueryCurrentInputTupleV2

    def __call__(
        self, context: Context, command: QueryCurrentInputTupleV2, *args, **kwargs
    ):
        # TODO: find a proper way to implement this handler. Right now it only
        #   returns an emtpy information as a placeholder
        return CurrentInputTupleInfo()
