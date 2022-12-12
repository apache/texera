import builtins
import datetime
import inspect
from contextlib import redirect_stdout
from io import StringIO

from typing import ContextManager

from core.util.buffer.buffer_base import IBuffer
from proto.edu.uci.ics.amber.engine.architecture.worker import PythonConsoleMessageV2


class replace_print(ContextManager):
    """
    A context manager to support replace builtin print function.

    With in the context, we use a customized print function which does the following:
    1. writes to a given buffer instead of stdout
    2. writes as a complete string, which is made of joining of all stringify-ed
    arguments and the end argument of the original print function. It calls the
    buf.write once per print call, which is different from
    contextlib.redirect_stdout who calls the buf.write for each argument in the
    print function.
    """

    def __init__(self, buf: IBuffer):
        # save a reference to the original builtin.print before we replace it.
        # it will always replace back when the context manager exits, with exception
        # or not.
        self.builtins_print = builtins.print

        self.buf = buf  # the provided buffer to write to

    def __enter__(self) -> None:
        """
        Enters the context, replace builtin.print function with a wrapped function.
        Now we hard code the wrapped_print to output complete print result to the
        given buffer.
        :return:
        """

        def wrapped_print(*args, **kwargs):
            # use StringIO to obtain the written complete string from the original
            # print function.
            if "file" in kwargs:
                self.builtins_print(*args, **kwargs)
                return
            with StringIO() as tmp_buf, redirect_stdout(tmp_buf):
                self.builtins_print(*args, **kwargs)
                complete_str = tmp_buf.getvalue()
                self.buf.put(
                    PythonConsoleMessageV2(
                        timestamp=datetime.datetime.now(),
                        msg_type="PRINT",
                        source=f"{inspect.currentframe().f_back.f_globals['__name__']}"
                        f":{inspect.currentframe().f_back.f_code.co_name}"
                        f":{inspect.currentframe().f_back.f_lineno}",
                        message=complete_str,
                    )
                )

        builtins.print = wrapped_print

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """
        Exits the context, revert the replacement to recover the original
        builtin.print function.

        It does not handle exception within the context, it simply raises it outside
        the context.

        :param exc_type: potential exception type.
        :param exc_val: potential exception value.
        :param exc_tb: potential exception traceback.
        :return: bool, if no exception was raised, return True, otherwise,
        return False.
        """
        builtins.print = self.builtins_print
        return exc_val is None
