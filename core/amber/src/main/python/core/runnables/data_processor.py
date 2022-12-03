import sys
from pdb import Pdb
from threading import Event

from loguru import logger

from core.architecture.managers import Context
from core.models import Tuple
from core.util import Stoppable
from core.util.console_message.replace_print import replace_print
from core.util.runnable.runnable import Runnable


class DataProcessor(Runnable, Stoppable):
    def __init__(self, context: Context):
        self._running = Event()
        self._context = context
        self._debugger = Pdb(
            stdin=self._context.debug_manager.debug_in,
            stdout=self._context.debug_manager.debug_out,
        )

    def run(self) -> None:
        with self._context.tuple_processing_manager.context_switch_condition:
            self._context.tuple_processing_manager.context_switch_condition.wait()
        self._running.set()
        self._switch_context()
        while self._running.is_set():
            self.process_tuple()
            self._switch_context()

    def process_tuple(self):
        finished_current = self._context.tuple_processing_manager.finished_current
        while not finished_current.is_set():

            try:
                operator = self._context.operator_manager.operator
                tuple_ = self._context.tuple_processing_manager.current_input_tuple
                link = self._context.tuple_processing_manager.current_input_link

                # bind link with input index
                # TODO: correct this with the actual port information.
                if link not in self._context.tuple_processing_manager.input_link_map:
                    self._context.tuple_processing_manager.input_links.append(link)
                    index = len(self._context.tuple_processing_manager.input_links) - 1
                    self._context.tuple_processing_manager.input_link_map[link] = index
                port = self._context.tuple_processing_manager.input_link_map[link]

                output_iterator = (
                    operator.process_tuple(tuple_, port)
                    if isinstance(tuple_, Tuple)
                    else operator.on_finish(port)
                )
                with replace_print(self._context.console_message_manager.print_buf):
                    for output in output_iterator:
                        self._context.tuple_processing_manager.current_output_tuple = (
                            None if output is None else Tuple(output)
                        )
                        self._switch_context()

                # current tuple finished successfully
                finished_current.set()

            except Exception as err:
                logger.exception(err)
                self._context.exception_manager.set_exception_info(sys.exc_info())
            finally:
                self._switch_context()

    def _switch_context(self):
        with self._context.tuple_processing_manager.context_switch_condition:
            self._context.debug_manager.talk_with_debugger.clear()
            self._context.tuple_processing_manager.context_switch_condition.notify()
            self._context.tuple_processing_manager.context_switch_condition.wait()
            logger.info("in dp")
        if (
            not self._context.debug_manager.talk_with_debugger.is_set()
            and self._context.debug_manager.debug_in.value is not None
        ):
            logger.info("bring up pdb")
            self._context.debug_manager.talk_with_debugger.set()
            self._debugger.set_trace()

    def stop(self):
        self._running.clear()
