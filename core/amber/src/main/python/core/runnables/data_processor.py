import typing
from typing import Iterator, Optional, Union

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
    LinkIdentity


class DataProcessor(StoppableQueueBlockingRunnable):

    def __init__(self, input_queue: InternalQueue, output_queue: InternalQueue, udf_operator: UDFOperator):
        super().__init__(self.__class__.__name__, queue=input_queue)

        self._input_queue: InternalQueue = input_queue
        self._output_queue: InternalQueue = output_queue
        self._udf_operator: UDFOperator = udf_operator
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

        # logger.info(f"PYTHON DP receive an entry from queue: {next_entry}")
        match(
            next_entry,
            DataElement, self._process_input_data_element,
            ControlElement, self._process_control_element,
            EndMarker, self._process_end_marker,
            EndOfAllMarker, self._process_end_of_all_marker
        )

    def process_control_command(self, tag: ActorVirtualIdentity, payload: ControlPayloadV2):
        # logger.info(f"PYTHON DP processing one CONTROL: {cmd} from {from_}")
        match(
            (tag, get_one_of(payload)),
            typing.Tuple[ActorVirtualIdentity, ControlInvocationV2], self._process_control_invocation
            # TODO: handle ReturnPayload
        )

    def process_input_tuple(self):
        if isinstance(self._current_input_tuple, Tuple):
            self.context.statistics_manager.increase_input_tuple_count()

        for result in self.process_tuple(self._current_input_tuple, self._current_input_link):
            self.context.statistics_manager.increase_output_tuple_count()
            self.pass_tuple_to_downstream(result)

    def process_tuple(self, tuple_: Union[Tuple, InputExhausted], link: LinkIdentity) -> Iterator[Tuple]:
        return self._udf_operator.process_texera_tuple(tuple_, link)

    def pass_tuple_to_downstream(self, tuple_: Tuple) -> None:
        for to, batch in self.context.tuple_to_batch_converter.tuple_to_batch(tuple_):
            self._output_queue.put(DataElement(tag=to, payload=batch))

    def complete(self) -> None:
        self._udf_operator.close()
        self.context.state_manager.transit_to(WorkerState.COMPLETED)
        control_command = set_one_of(ControlCommandV2, WorkerExecutionCompletedV2())
        self._async_rpc_client.send(ActorVirtualIdentity(name="CONTROLLER"), control_command)

    def check_and_process_control(self) -> None:

        while not self._input_queue.main_empty() or self.context.pause_manager.is_paused():
            next_entry = self.interruptible_get()

            match(
                next_entry,
                ControlElement, self._process_control_element
            )

    def _process_control_element(self, control_element: ControlElement) -> None:
        # logger.info(f"PYTHON DP receive a CONTROL: {next_entry}")
        self.process_control_command(control_element.tag, control_element.payload)

    def _process_tuple(self, tuple_: Tuple) -> None:
        self._current_input_tuple = tuple_
        self.process_input_tuple()
        self.check_and_process_control()

    def _process_sender_change_marker(self, sender_change_marker: SenderChangeMarker) -> None:
        self._current_input_link = sender_change_marker.link

    def _process_end_marker(self, _: EndMarker) -> None:
        self._current_input_tuple = InputExhausted()
        self.process_input_tuple()
        self.check_and_process_control()

    def _process_end_of_all_marker(self, _: EndOfAllMarker) -> None:
        for to, batch in self.context.tuple_to_batch_converter.emit_end_of_upstream():
            self._output_queue.put(DataElement(tag=to, payload=batch))
            self.check_and_process_control()
        self.complete()

    def _process_input_data_element(self, input_data_element: DataElement) -> None:
        if self.context.state_manager.confirm_state(WorkerState.READY):
            self.context.state_manager.transit_to(WorkerState.RUNNING)
        for element in self.context.batch_to_tuple_converter.process_data_payload(
                input_data_element.tag, input_data_element.payload):
            match(
                element,
                Tuple, self._process_tuple,
                SenderChangeMarker, self._process_sender_change_marker,
                EndMarker, self._process_end_marker,
                EndOfAllMarker, self._process_end_of_all_marker
            )

    def _process_control_invocation(self, control_invocation: ControlInvocationV2, from_: ActorVirtualIdentity) -> None:
        self._async_rpc_server.receive(control_invocation=control_invocation, from_=from_)
