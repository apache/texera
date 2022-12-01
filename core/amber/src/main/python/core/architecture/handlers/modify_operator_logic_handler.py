from loguru import logger

from proto.edu.uci.ics.amber.engine.architecture.worker import ModifyOperatorLogicV2
from .handler_base import Handler
from ..managers.context import Context


class ModifyOperatorLogicHandler(Handler):
    cmd = ModifyOperatorLogicV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        context.operator_manager.update_operator(command.code, command.is_source)
        # TODO: to be moved to message manager
        logger.add(
            context.main_loop._print_log_handler,
            level="PRINT",
            filter=context.operator_manager.operator_module_name,
            format="{message}",
        )
        return None
