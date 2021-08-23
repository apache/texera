import inspect
from importlib import util

from loguru import logger

from proto.edu.uci.ics.amber.engine.architecture.worker import ModifyPythonLogicV2
from .handler_base import Handler
from ..managers.context import Context
from ...udf import UDFOperator


class ModifyPythonLogicHandler(Handler):
    cmd = ModifyPythonLogicV2

    def __call__(self, context: Context, command: ModifyPythonLogicV2, *args, **kwargs):
        original_internal_state = context.dp._udf_operator.__dict__
        spec = util.spec_from_loader('udf_module', loader=None)
        udf_module = util.module_from_spec(spec)
        exec(command.code, udf_module.__dict__)
        operators = list(filter(lambda v: inspect.isclass(v)
                                          and issubclass(v, UDFOperator)
                                          and not inspect.isabstract(v),
                                udf_module.__dict__.values()))
        logger.info(f"got operators {operators}")
        assert len(operators) == 1, "There should be only one UDFOperator defined"
        context.dp._udf_operator = operators[0]()
        # overwrite the internal state
        context.dp._udf_operator.__dict__ = original_internal_state
        return None
