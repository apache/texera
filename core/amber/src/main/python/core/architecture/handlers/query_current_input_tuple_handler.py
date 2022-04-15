from proto.edu.uci.ics.amber.engine.architecture.worker import (
    CurrentInputTupleInfo,
    QueryCurrentInputTupleV2,
)
from .handler_base import Handler
from ..managers.context import Context


class QueryCurrentInputTupleHandler(Handler):
    cmd = QueryCurrentInputTupleV2

    def __call__(
        self, context: Context, command: QueryCurrentInputTupleV2, *args, **kwargs
    ):
        # TODO: find a proper way to implement this handler. Right now it only
        #   returns an emtpy information as a placeholder
        return CurrentInputTupleInfo()
