from core.architecture.handlers.handler_base import Handler
from core.architecture.managers.context import Context
from proto.edu.uci.ics.amber.engine.architecture.worker import UpdateInputLinkingV2


class UpdateInputLinkingHandler(Handler):
    cmd = UpdateInputLinkingV2

    def __call__(
        self, context: Context, command: UpdateInputLinkingV2, *args, **kwargs
    ):
        context.batch_to_tuple_converter.register_input(
            command.identifier, command.input_link
        )
        return None
