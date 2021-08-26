from proto.edu.uci.ics.amber.engine.architecture.worker import ModifyPythonLogicV2
from .handler_base import Handler
from ..managers.context import Context
from ...udf import UDFOperator
from ...udf.udf_util import load_udf


class ModifyPythonLogicHandler(Handler):
    cmd = ModifyPythonLogicV2

    def __call__(self, context: Context, command: ModifyPythonLogicV2, *args, **kwargs):
        original_internal_state = context.dp._udf_operator.__dict__
        udf_operator: type(UDFOperator) = load_udf(command.code)
        context.dp._udf_operator = udf_operator()
        # overwrite the internal state
        context.dp._udf_operator.__dict__ = original_internal_state
        return None
