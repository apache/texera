from loguru import logger

from proto.edu.uci.ics.amber.engine.architecture.worker import (
    StateRequestV2,
    StateReturn,
)
from .handler_base import Handler
from ..managers.context import Context
import pickle


class StateRequestHandler(Handler):
    cmd = StateRequestV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        # logger.info("trying to send back a state for " + str(command.line_no) + " "
        #             + command.tuple_id + " " +
        #             command.state_name)
        state = context.debug_manager.states[
            (command.tuple_id, str(command.line_no), command.state_name)
        ]
        # del context.debug_manager.states[(command.tuple_id, str(command.line_no),
        #                                   command.state_name)]
        return StateReturn(bytes=str(state))
