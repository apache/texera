from dataclasses import dataclass
from queue import Queue

from loguru import logger

from .control_payload import ControlPayload
from .data_tuple import DataTuple
from .stoppable_thread import StoppableThread


@dataclass
class InternalQueueElement:
    pass


@dataclass
class InputTuple(InternalQueueElement):
    tuple: DataTuple


@dataclass
class ControlElement(InternalQueueElement):
    cmd: ControlPayload


@dataclass
class EndMarker(InternalQueueElement):
    pass


@dataclass
class EndOfAllMarker(InternalQueueElement):
    pass


class DataProcessor(StoppableThread):
    def __init__(self, input_queue: Queue, output_queue: Queue):
        super().__init__(self.__class__.__name__)
        self._input_queue = input_queue
        self._output_queue = output_queue

    def run(self) -> None:
        while self.running():
            next_entry = self._input_queue.get()
            if isinstance(next_entry, InputTuple):
                self.handle_input_tuple(next_entry.tuple)
            elif isinstance(next_entry, ControlElement):
                self.process_control_command(next_entry.cmd)

    def handle_input_tuple(self, data_tuple: DataTuple):
        logger.info(f"processing one tuple {data_tuple}")
        self._output_queue.put(InputTuple(tuple=data_tuple))

    @staticmethod
    def process_control_command(cmd: ControlPayload):
        logger.info(f"processing one control {cmd}")
