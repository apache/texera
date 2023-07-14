import pickle

from proto.edu.uci.ics.amber.engine.architecture.worker import (
    StateRequestV2,
    StateReturn,
)
from .handler_base import Handler
from ..managers.context import Context


class StateRequestHandler(Handler):
    cmd = StateRequestV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        state_key = (command.tuple_id, str(command.line_no), command.state_name)
        state = context.debug_manager.states[state_key]

        # the state can only be request once and cleared after the request.
        del context.debug_manager.states[state_key]

        return StateReturn(bytes=pickle.dumps(state))
