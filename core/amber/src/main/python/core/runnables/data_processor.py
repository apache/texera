from queue import Queue
from threading import Event

from loguru import logger

from core.architecture.managers import Context
from core.models import Tuple
from core.util.runnable.runnable import Runnable


class DataProcessor(Runnable):
    def __init__(
        self, input_queue: Queue, output_queue: Queue, dp_condition, context: Context
    ):
        self._input_queue = input_queue
        self._output_queue = output_queue
        self._dp_condition = dp_condition
        self._finished_current = Event()
        self._running = Event()
        self._context = context

    def run(self) -> None:
        with self._dp_condition:
            self._dp_condition.wait()
        self._running.set()
        self.context_switch()

        while self._running.is_set():
            tuple_, port = self._input_queue.get()
            self.process_tuple(tuple_, port)
            self.context_switch()

    def process_tuple(self, tuple_: Tuple, port: int):
        while not self._finished_current.is_set():
            try:
                operator = self._context.operator_manager.operator
                output_iterator = (
                    operator.process_tuple(tuple_, port)
                    if isinstance(tuple_, Tuple)
                    else operator.on_finish(port)
                )
                for output in output_iterator:
                    self._output_queue.put(None if output is None else Tuple(output))
                    self.context_switch()

                # current tuple finished successfully
                self._finished_current.set()

            except Exception as err:
                logger.exception(err)
                self._context.main_loop.report_exception()
                self._context.main_loop._pause()
                self.context_switch()

    def context_switch(self):
        with self._dp_condition:
            self._dp_condition.notify()
            self._dp_condition.wait()

    def stop(self):
        self._running.clear()
