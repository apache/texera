import os
import sys
import traceback
from datetime import datetime
from threading import Event

from loguru import logger

from core.architecture.managers import Context
from core.models import Tuple, ExceptionInfo
from core.models.table import all_output_to_tuple
from core.util import Stoppable
from core.util.console_message.replace_print import replace_print
from core.util.runnable.runnable import Runnable
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    ConsoleMessage,
    ConsoleMessageType,
)


class DataProcessor(Runnable, Stoppable):
    def __init__(self, context: Context):
        self._running = Event()
        self._context = context

    def run(self) -> None:
        with self._context.tuple_processing_manager.context_switch_condition:
            self._context.tuple_processing_manager.context_switch_condition.wait()
        self._running.set()
        self._switch_context()
        while self._running.is_set():
            self.process_tuple()
            self._switch_context()

    def process_tuple(self) -> None:
        finished_current = self._context.tuple_processing_manager.finished_current
        while not finished_current.is_set():
            try:
                executor = self._context.executor_manager.executor
                tuple_ = self._context.tuple_processing_manager.current_input_tuple
                port_id = self._context.tuple_processing_manager.current_input_port_id
                port: int
                if port_id is None:
                    # no upstream, special case for source executor.
                    port = 0
                else:
                    port = port_id.id

                output_iterator = (
                    executor.process_tuple(tuple_, port)
                    if isinstance(tuple_, Tuple)
                    else executor.on_finish(port)
                )
                with replace_print(
                    self._context.worker_id,
                    self._context.console_message_manager.print_buf,
                ):
                    for output in output_iterator:
                        # output could be a None, a TupleLike, or a TableLike.
                        for output_tuple in all_output_to_tuple(output):
                            self._set_output_tuple(output_tuple)
                            self._switch_context()

                # current tuple finished successfully
                finished_current.set()

            except Exception as err:
                logger.exception(err)
                exc_info = sys.exc_info()
                self._context.exception_manager.set_exception_info(exc_info)
                self._report_exception(exc_info)

            finally:
                self._switch_context()

    def _set_output_tuple(self, output_tuple):
        if output_tuple is not None:
            output_tuple.finalize(self._context.output_manager.get_port().get_schema())
        self._context.tuple_processing_manager.current_output_tuple = output_tuple

    def _switch_context(self) -> None:
        """
        Notify the MainLoop thread and wait here until being switched back.
        """
        with self._context.tuple_processing_manager.context_switch_condition:
            self._context.tuple_processing_manager.context_switch_condition.notify()
            self._context.tuple_processing_manager.context_switch_condition.wait()
        self._post_switch_context_checks()

    def _check_and_process_debug_command(self) -> None:
        """
        If a debug command is available, invokes the debugger from this frame.
        """
        if self._context.debug_manager.has_debug_command():
            # Let debugger trace from the current frame.
            # This line will also trigger cmdloop in the debugger.
            # This line has no side effects on the current debugger state.
            self._context.debug_manager.debugger.set_trace()

    def _post_switch_context_checks(self):
        self._check_and_process_debug_command()

    def _report_exception(self, exc_info: ExceptionInfo):
        tb = traceback.extract_tb(exc_info[2])
        filename, line_number, func_name, text = tb[-1]
        base_name = os.path.basename(filename)
        module_name, _ = os.path.splitext(base_name)
        formatted_exception = traceback.format_exception(*exc_info)
        title: str = formatted_exception[-1].strip()
        message: str = "\n".join(formatted_exception)

        self._context.console_message_manager.put_message(
            ConsoleMessage(
                worker_id=self._context.worker_id,
                timestamp=datetime.now(),
                msg_type=ConsoleMessageType.ERROR,
                source=f"{module_name}:{func_name}:{line_number}",
                title=title,
                message=message,
            )
        )

    def stop(self):
        self._running.clear()
