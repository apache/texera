import typing
from queue import Queue
from typing import Iterable, Union

from loguru import logger

from worker.models.control_payload import ControlPayload
from worker.models.generated.virtualidentity_pb2 import LinkIdentity, ActorVirtualIdentity

from worker.models.internal_queue import InputTuple, ControlElement, SenderChangeMarker, EndMarker, EndOfAllMarker
from worker.models.tuple import ITuple, InputExhausted, Tuple
from worker.udf.udf_operator import UDFOperator
from worker.util.stoppable_queue_blocking_thread import StoppableQueueBlockingThread


class BatchProducer:
    pass


class DPThread(StoppableQueueBlockingThread):
    def __init__(self, input_queue: Queue, output_queue: Queue, udf_operator: UDFOperator):
        super().__init__(self.__class__.__name__, queue=input_queue)

        self._input_queue = input_queue
        self._output_queue = output_queue
        self._udf_operator = udf_operator
        self._current_input_tuple: Union[ITuple, InputExhausted]
        self._current_input_link: LinkIdentity
        self.output_tuple_count: int = 0
        self.input_tuple_count: int = 0
        self._batch_producer = BatchProducer()

    def before_loop(self) -> None:
        self._udf_operator.open()

    def after_loop(self) -> None:
        self._udf_operator.close()

    def main_loop(self) -> None:
        next_entry = self.interruptible_get()
        logger.debug(f"getting {next_entry}")
        if isinstance(next_entry, InputTuple):
            self._current_input_tuple = next_entry.tuple
            self.handle_input_tuple()
        elif isinstance(next_entry, SenderChangeMarker):
            self._current_input_link = next_entry.link
        elif isinstance(next_entry, EndMarker):
            self._current_input_tuple = InputExhausted()
            self.handle_input_tuple()
        elif isinstance(next_entry, EndOfAllMarker):
            # TODO: in original design, when receiving EndOfAllMarker,
            #  it needs to send InputExhausted() to all downstream actors.
            #  Here we do not have such information.
            #  we should instead send a control message back to Java side,
            #  to invoke `batchProducer.emitEndOfUpstream()`
            self.stop()
        elif isinstance(next_entry, ControlElement):
            self.process_control_command(next_entry.cmd, next_entry.from_)
        else:
            raise TypeError(f"unknown InternalQueueElement {next_entry}")

    @staticmethod
    def process_control_command(cmd: ControlPayload, from_: ActorVirtualIdentity):
        logger.info(f"processing one control {cmd} from {from_}")

    def handle_input_tuple(self):
        results: Iterable[ITuple] = self.process_tuple(self._current_input_tuple, self._current_input_link)
        if isinstance(self._current_input_tuple, ITuple):
            self.input_tuple_count += 1
        for result in results:
            self.output_one_tuple(result)

    def output_one_tuple(self, tuple_: ITuple):
        self.output_tuple_count += 1
        self._output_queue.put(InputTuple(tuple_))

    def process_tuple(self, tuple_: Union[ITuple, InputExhausted], link: LinkIdentity) -> Iterable[ITuple]:
        typing.cast(tuple_, Union[Tuple, InputExhausted])
        return self._udf_operator.process_texera_tuple(tuple_, link)
