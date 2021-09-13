from core.udf.udf_util import load_udf
from proto.edu.uci.ics.amber.engine.architecture.worker import InitializeOperatorLogicV2
from .handler_base import Handler
from ..managers.context import Context
from ...udf import UDFOperator


class InitializeOperatorLogicHandler(Handler):
    cmd = InitializeOperatorLogicV2

    def __call__(self, context: Context, command: cmd, *args, **kwargs):
        udf_operator: type(UDFOperator) = load_udf(command.code)
        context.dp._udf_operator = udf_operator()
        context.dp._udf_operator.is_source = command.is_source
        context.dp._udf_operator.open()
        return None
