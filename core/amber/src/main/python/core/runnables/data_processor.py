import typing
from typing import Iterator, Optional, Union

from loguru import logger
from overrides import overrides
from pampy import match

from core.architecture.managers.context import Context
from core.architecture.packaging.batch_to_tuple_converter import EndMarker, EndOfAllMarker
from core.architecture.rpc.async_rpc_client import AsyncRPCClient
from core.architecture.rpc.async_rpc_server import AsyncRPCServer
from core.models.internal_queue import ControlElement, DataElement, InternalQueue
from core.models.marker import SenderChangeMarker
from core.models.tuple import InputExhausted, Tuple
from core.udf.udf_operator import UDFOperator
from core.util import IQueue, StoppableQueueBlockingRunnable, get_one_of, set_one_of
from proto.edu.uci.ics.amber.engine.architecture.worker import ControlCommandV2, WorkerExecutionCompletedV2, WorkerState
from proto.edu.uci.ics.amber.engine.common import ActorVirtualIdentity, ControlInvocationV2, ControlPayloadV2, \
    LinkIdentity, ReturnInvocationV2


class DataProcessor(StoppableQueueBlockingRunnable):

    def __init__(self, input_queue: InternalQueue, output_queue: InternalQueue):
        super().__init__(self.__class__.__name__, queue=input_queue)

        self._input_queue: InternalQueue = input_queue
        self._output_queue: InternalQueue = output_queue
        self._udf_operator: Optional[UDFOperator] = None
        self._current_input_tuple: Optional[Union[Tuple, InputExhausted]] = None
        self._current_input_link: Optional[LinkIdentity] = None

        self.context = Context(self)
        self._async_rpc_server = AsyncRPCServer(output_queue, context=self.context)
        self._async_rpc_client = AsyncRPCClient(output_queue, context=self.context)

    @overrides
    def pre_start(self) -> None:
        self.context.state_manager.assert_state(WorkerState.UNINITIALIZED)
        self.context.state_manager.transit_to(WorkerState.READY)

    @overrides
    def receive(self, next_entry: IQueue.QueueElement) -> None:
        """
        Main entry point of the DataProcessor. Upon receipt of an next_entry, process it respectfully.

        :param next_entry: An entry from input_queue, could be one of the followings:
                    1. a ControlElement;
                    2. a DataElement.
        """
        match(
            next_entry,
            DataElement, self._process_data_element,
            ControlElement, self._process_control_element,
            EndMarker, self._process_end_marker,
            EndOfAllMarker, self._process_end_of_all_marker
        )

    def process_control_payload(self, tag: ActorVirtualIdentity, payload: ControlPayloadV2) -> None:
        """
        Process the given ControlPayload with the tag.
        :param tag: ActorVirtualIdentity, the sender.
        :param payload: ControlPayloadV2 to be handled.
        """
        # logger.debug(f"processing one CONTROL: {payload} from {tag}")
        match(
            (tag, get_one_of(payload)),
            typing.Tuple[ActorVirtualIdentity, ControlInvocationV2], self._async_rpc_server.receive,
            typing.Tuple[ActorVirtualIdentity, ReturnInvocationV2], self._async_rpc_client.receive
        )

    def process_input_tuple(self) -> None:
        """
        Process the current input tuple with the current input link. Send all result Tuples
        to downstream operators.

        This is being invoked for each Tuple/Marker that are unpacked from the DataElement.
        """
        if isinstance(self._current_input_tuple, Tuple):
            self.context.statistics_manager.increase_input_tuple_count()

        for tuple_ in self.process_tuple(self._current_input_tuple, self._current_input_link):
            self.context.statistics_manager.increase_output_tuple_count()
            for to, batch in self.context.tuple_to_batch_converter.tuple_to_batch(tuple_):
                self._output_queue.put(DataElement(tag=to, payload=batch))

    def process_tuple(self, tuple_: Union[Tuple, InputExhausted], link: LinkIdentity) -> Iterator[Tuple]:
        """
        Process the Tuple/InputExhausted with the current link.

        This is a wrapper to invoke udf operator.

        :param tuple_: Union[Tuple, InputExhausted], the current tuple.
        :param link: LinkIdentity, the current link.
        :return: Iterator[Tuple], iterator of result Tuple(s).
        """
        return self._udf_operator.process_texera_tuple(tuple_, link)

    def complete(self) -> None:
        """
        Complete the DataProcessor, marking state to COMPLETED, and notify the controller.
        """
        self._udf_operator.close()
        self.context.state_manager.transit_to(WorkerState.COMPLETED)
        control_command = set_one_of(ControlCommandV2, WorkerExecutionCompletedV2())
        self._async_rpc_client.send(ActorVirtualIdentity(name="CONTROLLER"), control_command)

    def check_and_process_control(self) -> None:
        """
        Check if there exists any ControlElement(s) in the input_queue, if so, take and process
        them one by one.

        This is used very frequently as we want to prioritize the process of ControlElement ,
        and will be invoked many times during a DataElement's processing lifecycle. Thus, this
        method's invocation could appear in any stage while processing a DataElement.
        """

        while not self._input_queue.main_empty() or self.context.pause_manager.is_paused():
            next_entry = self.interruptible_get()
            self._process_control_element(next_entry)

    def _process_control_element(self, control_element: ControlElement) -> None:
        """
        Upon receipt of a ControlElement, unpack it into tag and payload to be handled.

        :param control_element: ControlElement to be handled.
        """
        self.process_control_payload(control_element.tag, control_element.payload)

    def _process_tuple(self, tuple_: Tuple) -> None:
        self._current_input_tuple = tuple_
        self.process_input_tuple()
        self.check_and_process_control()

    def _process_sender_change_marker(self, sender_change_marker: SenderChangeMarker) -> None:
        """
        Upon receipt of a SenderChangeMarker, change the current input link to the sender.

        :param sender_change_marker: SenderChangeMarker which contains sender link.
        """
        self._current_input_link = sender_change_marker.link

    def _process_end_marker(self, _: EndMarker) -> None:
        """
        Upon receipt of an EndMarker, which indicates the end of the current input link.
        process an InputExhausted for the current input link.

        :param _: EndMarker
        """
        self._current_input_tuple = InputExhausted()
        self.process_input_tuple()
        self.check_and_process_control()

    def _process_end_of_all_marker(self, _: EndOfAllMarker) -> None:
        """
        Upon receipt of an EndOfAllMarker, which indicates the end of all input links,
        send the last data batches to all downstream workers.

        It will also invoke complete() of this DataProcessor.

        :param _: EndOfAllMarker
        """
        for to, batch in self.context.tuple_to_batch_converter.emit_end_of_upstream():
            self._output_queue.put(DataElement(tag=to, payload=batch))
            self.check_and_process_control()
        self.complete()

    def _process_data_element(self, data_element: DataElement) -> None:
        """
        Upon receipt of a DataElement, unpack it into Tuples and Markers,
        and process them one by one.

        :param data_element: DataElement, a batch of data.
        """

        # Update state to RUNNING
        if self.context.state_manager.confirm_state(WorkerState.READY):
            self.context.state_manager.transit_to(WorkerState.RUNNING)

        for element in self.context.batch_to_tuple_converter.process_data_payload(
                data_element.tag, data_element.payload):
            match(
                element,
                Tuple, self._process_tuple,
                SenderChangeMarker, self._process_sender_change_marker,
                EndMarker, self._process_end_marker,
                EndOfAllMarker, self._process_end_of_all_marker
            )
