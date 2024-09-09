import sys
from threading import Event
from typing import Optional

from loguru import logger

from core.architecture.managers import Context
from core.models import Tuple, TupleLike
from core.util import Stoppable
from core.util.console_message.replace_print import replace_print
from core.util.runnable.runnable import Runnable


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
        current_input = self._context.tuple_processing_manager.current_input
        try:
            while not finished_current.is_set():
                if isinstance(current_input, Tuple):
                    self._context.debug_manager.optimize_debugger(current_input)
                operator = self._context.operator_manager.operator
                tuple_ = self._context.tuple_processing_manager.current_input
                link = self._context.tuple_processing_manager.current_input_link
                port: int
                if link is None:
                    # no upstream, special case for source operator.
                    port = -1

                elif link in self._context.tuple_processing_manager.input_link_map:
                    port = self._context.tuple_processing_manager.input_link_map[link]

                else:
                    raise Exception(
                        f"Unexpected input link {link}, not in input mapping "
                        f"{self._context.tuple_processing_manager.input_link_map}"
                    )

                self._context.tuple_processing_manager.output_iterator = (
                    operator.process_tuple(tuple_, port)
                    if isinstance(tuple_, Tuple)
                    else operator.on_finish(port)
                )
                with replace_print(self._context.console_message_manager.print_buf):
                    for output in self._context.tuple_processing_manager.output_iterator:
                        if not isinstance(output, str):
                            # the output is a TupleLike

                            ret = self.create_output_tuple(output)
                        else:
                            # the output is a state transfer statement
                            ret = output
                        self._context.tuple_processing_manager.current_output = ret

                        self._switch_context()

                # current tuple finished successfully
                finished_current.set()

        except Exception as err:
            logger.exception(err)
            self._context.exception_manager.set_exception_info(sys.exc_info())
        finally:
            self._switch_context()

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
        while self._context.debug_manager.has_debug_command():
            # Let debugger trace from the current frame.
            # This line will also trigger cmdloop in the debugger.
            # This line has no side effects on the current debugger state.
            self._context.debug_manager.debugger.set_trace()

    def _post_switch_context_checks(self):
        self._check_and_process_debug_command()

    def stop(self):
        self._running.clear()

    def create_output_tuple(self, output: TupleLike) -> Optional[Tuple]:
        output_tuple = None if output is None else Tuple(output)
        if output_tuple is not None:
            output_tuple.finalize(self._context.operator_manager.operator.output_schema)
        return output_tuple
