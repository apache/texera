import inspect
from importlib import util

from loguru import logger

from proto.edu.uci.ics.amber.engine.architecture.worker import SendPythonUdfV2
from .handler_base import Handler
from ..managers.context import Context
from ...models import UDFOperator


class SendPythonUdfHandler(Handler):
    cmd = SendPythonUdfV2

    def __call__(self, context: Context, command: SendPythonUdfV2, *args, **kwargs):
        spec = util.spec_from_loader('udf_module', loader=None)
        udf_module = util.module_from_spec(spec)
        exec(command.udf, udf_module.__dict__)
        operators = list(filter(lambda v: inspect.isclass(v)
                                          and issubclass(v, UDFOperator)
                                          and not inspect.isabstract(v),
                                udf_module.__dict__.values()))
        logger.info(f"got operators {operators}")
        assert len(operators) == 1, "There should be only one UDFOperator defined"
        context.dp._udf_operator = operators[0]()
        context.dp._udf_operator.is_source = command.is_source
        context.dp._udf_operator.open()
        return None
