from proto.edu.uci.ics.amber.engine.architecture.worker import InitializeOperatorLogicV2
from .handler_base import Handler
from ..managers.context import Context
from ...models import Operator
from ...util.operator import load_operator


class InitializeOperatorLogicHandler(Handler):
    cmd = InitializeOperatorLogicV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        operator: type(Operator) = load_operator(command.code)
        context.operator_manager.operator = operator()
        context.operator_manager.operator.is_source = command.is_source
        context.operator_manager.operator.output_schema = command.output_schema
        context.batch_to_tuple_converter.update_all_upstream_link_ids(
            set(command.upstream_link_ids)
        )
        return None
