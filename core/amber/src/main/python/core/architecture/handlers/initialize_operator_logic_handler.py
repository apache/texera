from loguru import logger

from proto.edu.uci.ics.amber.engine.architecture.worker import InitializeOperatorLogicV2
from .handler_base import Handler
from ..managers.context import Context


class InitializeOperatorLogicHandler(Handler):
    cmd = InitializeOperatorLogicV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        context.operator_manager.initialize_operator(
            command.code, command.is_source, command.output_schema
        )
        context.batch_to_tuple_converter.update_all_upstream_link_ids(
            set(command.upstream_link_ids)
        )
        return None
