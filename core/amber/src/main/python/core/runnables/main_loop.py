import datetime
import threading
import time
import typing
from typing import Iterator, Optional, Union

from loguru import logger
from overrides import overrides
from pampy import match

from core.architecture.managers.context import Context
from core.architecture.managers.pause_manager import PauseType
from core.architecture.packaging.input_manager import EndOfAll
from core.architecture.rpc.async_rpc_client import AsyncRPCClient
from core.architecture.rpc.async_rpc_server import AsyncRPCServer
from core.models import (
    InputExhausted,
    InternalQueue,
    SenderChange,
    Tuple,
)
from core.models.internal_queue import DataElement, ControlElement
from core.runnables.data_processor import DataProcessor
from core.util import StoppableQueueBlockingRunnable, get_one_of, set_one_of
from core.util.customized_queue.queue_base import QueueElement
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    ControlCommandV2,
    ConsoleMessageType,
    WorkerExecutionCompletedV2,
    WorkerState,
    PythonConsoleMessageV2,
    ConsoleMessage,
    PortCompletedV2,
)
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    ControlInvocationV2,
    ControlPayloadV2,
    ReturnInvocationV2,
    PortIdentity,
)


class MainLoop(StoppableQueueBlockingRunnable):
    def __init__(
        self,
        worker_id: str,
        input_queue: InternalQueue,
        output_queue: InternalQueue,
    ):
        super().__init__(self.__class__.__name__, queue=input_queue)
        self._input_queue: InternalQueue = input_queue
        self._output_queue: InternalQueue = output_queue

        self.context = Context(worker_id, input_queue)
        self._async_rpc_server = AsyncRPCServer(output_queue, context=self.context)
        self._async_rpc_client = AsyncRPCClient(output_queue, context=self.context)

        self.data_processor = DataProcessor(self.context)
        threading.Thread(
            target=self.data_processor.run, daemon=True, name="data_processor_thread"
        ).start()

    def complete(self) -> None:
        """
        Complete the DataProcessor, marking state to COMPLETED, and notify the
        controller.
        """
        # flush the buffered console prints
        self._check_and_report_console_messages(force_flush=True)
        self.context.executor_manager.executor.close()
        # stop the data processing thread
        self.data_processor.stop()
        self.context.state_manager.transit_to(WorkerState.COMPLETED)
        self.context.statistics_manager.update_total_execution_time(
            time.time_ns() - self.context.statistics_manager.worker_start_time
        )
        control_command = set_one_of(ControlCommandV2, WorkerExecutionCompletedV2())
        self._async_rpc_client.send(
            ActorVirtualIdentity(name="CONTROLLER"), control_command
        )
        self.context.close()

    def _check_and_process_control(self) -> None:
        """
        Check if there exists any ControlElement(s) in the input_queue, if so, take and
        process them one by one.

        This is used very frequently as we want to prioritize the process of
        ControlElement, and will be invoked many times during a DataElement's
        processing lifecycle. Thus, this method's invocation could appear in any
        stage while processing a DataElement.
        """
        while (
            not self._input_queue.is_control_empty()
            or not self._input_queue.is_data_enabled()
        ):
            next_entry = self.interruptible_get()
            self._process_control_element(next_entry)

    @overrides
    def pre_start(self) -> None:
        self.context.state_manager.assert_state(WorkerState.UNINITIALIZED)
        self.context.state_manager.transit_to(WorkerState.READY)
        self.context.statistics_manager.worker_start_time = time.time_ns()

    @overrides
    def receive(self, next_entry: QueueElement) -> None:
        """
        Main entry point of the DataProcessor. Upon receipt of an next_entry,
        process it respectfully.

        :param next_entry: An entry from input_queue, could be one of the followings:
                    1. a ControlElement;
                    2. a DataElement.
        """
        match(
            next_entry,
            DataElement,
            self._process_data_element,
            ControlElement,
            self._process_control_element,
        )

    def process_control_payload(
        self, tag: ActorVirtualIdentity, payload: ControlPayloadV2
    ) -> None:
        """
        Process the given ControlPayload with the tag.

        :param tag: ActorVirtualIdentity, the sender.
        :param payload: ControlPayloadV2 to be handled.
        """
        start_time = time.time_ns()
        match(
            (tag, get_one_of(payload)),
            typing.Tuple[ActorVirtualIdentity, ControlInvocationV2],
            self._async_rpc_server.receive,
            typing.Tuple[ActorVirtualIdentity, ReturnInvocationV2],
            self._async_rpc_client.receive,
        )
        end_time = time.time_ns()
        self.context.statistics_manager.increase_control_processing_time(
            end_time - start_time
        )
        self.context.statistics_manager.update_total_execution_time(
            end_time - self.context.statistics_manager.worker_start_time
        )

    def process_input_tuple(self) -> None:
        """
        Process the current input tuple with the current input link. Send all result
        Tuples to downstream workers.

        This is being invoked for each Tuple/Marker that are unpacked from the
        DataElement.
        """
        if isinstance(self.context.tuple_processing_manager.current_input_tuple, Tuple):
            self.context.statistics_manager.increase_input_tuple_count(
                self.context.tuple_processing_manager.current_input_port_id
            )

        for output_tuple in self.process_tuple_with_udf():
            self._check_and_process_control()
            if output_tuple is not None:
                self.context.statistics_manager.increase_output_tuple_count(
                    PortIdentity(0)
                )
                for to, batch in self.context.output_manager.tuple_to_batch(
                    output_tuple
                ):
                    self._output_queue.put(DataElement(tag=to, payload=batch))

    def process_tuple_with_udf(self) -> Iterator[Optional[Tuple]]:
        """
        Process the Tuple/InputExhausted with the current link.

        This is a wrapper to invoke processing of the executor.

        :return: Iterator[Tuple], iterator of result Tuple(s).
        """
        finished_current = self.context.tuple_processing_manager.finished_current
        finished_current.clear()

        while not finished_current.is_set():
            self._check_and_process_control()
            self._switch_context()
            yield self.context.tuple_processing_manager.get_output_tuple()

    def _process_control_element(self, control_element: ControlElement) -> None:
        """
        Upon receipt of a ControlElement, unpack it into tag and payload to be handled.

        :param control_element: ControlElement to be handled.
        """
        self.process_control_payload(control_element.tag, control_element.payload)

    def _process_tuple(self, tuple_: Union[Tuple, InputExhausted]) -> None:
        self.context.tuple_processing_manager.current_input_tuple = tuple_
        self.process_input_tuple()
        self._check_and_process_control()

    def _process_input_exhausted(self, input_exhausted: InputExhausted):
        self._process_tuple(input_exhausted)
        if self.context.tuple_processing_manager.current_input_port_id is not None:
            control_command = set_one_of(
                ControlCommandV2,
                PortCompletedV2(
                    self.context.tuple_processing_manager.current_input_port_id,
                    input=True,
                ),
            )
            self._async_rpc_client.send(
                ActorVirtualIdentity(name="CONTROLLER"), control_command
            )

    def _process_sender_change_marker(self, sender_change_marker: SenderChange) -> None:
        """
        Upon receipt of a SenderChangeMarker, change the current input link to the
        sender.

        :param sender_change_marker: SenderChangeMarker which contains sender link.
        """
        self.context.tuple_processing_manager.current_input_port_id = (
            self.context.input_manager.get_port_id(sender_change_marker.channel_id)
        )

    def _process_end_of_all_marker(self, _: EndOfAll) -> None:
        """
        Upon receipt of an EndOfAllMarker, which indicates the end of all input links,
        send the last data batches to all downstream workers.

        It will also invoke complete() of this DataProcessor.

        :param _: EndOfAllMarker
        """
        for to, batch in self.context.output_manager.emit_end_of_upstream():
            self._output_queue.put(DataElement(tag=to, payload=batch))
            self._check_and_process_control()
            control_command = set_one_of(
                ControlCommandV2,
                PortCompletedV2(PortIdentity(0), input=False),
            )
            self._async_rpc_client.send(
                ActorVirtualIdentity(name="CONTROLLER"), control_command
            )
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

        self.context.tuple_processing_manager.current_input_tuple_iter = (
            self.context.input_manager.process_data_payload(
                data_element.tag, data_element.payload
            )
        )

        if self.context.tuple_processing_manager.current_input_tuple_iter is None:
            return
        # here the self.context.processing_manager.current_input_tuple_iter
        # could be modified during iteration, thus we are using the while :=
        # way to iterate through the iterator, instead of the for-each-loop
        # syntax sugar.
        while (
            element := next(
                self.context.tuple_processing_manager.current_input_tuple_iter, None
            )
        ) is not None:
            try:
                match(
                    element,
                    Tuple,
                    self._process_tuple,
                    InputExhausted,
                    self._process_input_exhausted,
                    SenderChange,
                    self._process_sender_change_marker,
                    EndOfAll,
                    self._process_end_of_all_marker,
                )
            except Exception as err:
                logger.exception(err)

    def _scheduler_time_slot_event(self, time_slot_expired: bool) -> None:
        """
        The time slot for scheduling this worker has expired.
        """
        if time_slot_expired:
            self.context.pause_manager.pause(
                PauseType.SCHEDULER_TIME_SLOT_EXPIRED_PAUSE
            )
        else:
            self.context.pause_manager.resume(
                PauseType.SCHEDULER_TIME_SLOT_EXPIRED_PAUSE
            )

    def _send_console_message(self, console_message: PythonConsoleMessageV2):
        self._async_rpc_client.send(
            ActorVirtualIdentity(name="CONTROLLER"),
            set_one_of(
                ControlCommandV2,
                console_message,
            ),
        )

    def _switch_context(self) -> None:
        """
        Notify the DataProcessor thread and wait here until being switched back.
        """
        start_time = time.time_ns()
        with self.context.tuple_processing_manager.context_switch_condition:
            self.context.tuple_processing_manager.context_switch_condition.notify()
            self.context.tuple_processing_manager.context_switch_condition.wait()
        self._post_switch_context_checks()
        end_time = time.time_ns()
        self.context.statistics_manager.increase_data_processing_time(
            end_time - start_time
        )
        self.context.statistics_manager.update_total_execution_time(
            end_time - self.context.statistics_manager.worker_start_time
        )

    def _check_and_report_debug_event(self) -> None:
        if self.context.debug_manager.has_debug_event():
            debug_event = self.context.debug_manager.get_debug_event()
            self._send_console_message(
                PythonConsoleMessageV2(
                    ConsoleMessage(
                        worker_id=self.context.worker_id,
                        timestamp=datetime.datetime.now(),
                        msg_type=ConsoleMessageType.DEBUGGER,
                        source="(Pdb)",
                        title=debug_event,
                        message="",
                    )
                )
            )
            self._check_and_report_console_messages(force_flush=True)
            self.context.pause_manager.pause(PauseType.DEBUG_PAUSE)

    def _check_exception(self) -> None:
        if self.context.exception_manager.has_exception():
            self._check_and_report_console_messages(force_flush=True)
            self.context.pause_manager.pause(PauseType.EXCEPTION_PAUSE)

    def _check_and_report_console_messages(self, force_flush=False) -> None:
        for msg in self.context.console_message_manager.get_messages(force_flush):
            self._send_console_message(PythonConsoleMessageV2(msg))

    def _post_switch_context_checks(self) -> None:
        """
        Post callback for switch context.

        One step in DataProcessor could produce some results, which includes
            - print messages
            - Debug Event
            - Exception
        We check and report them each time coming back from DataProcessor.
        """
        self._check_and_report_console_messages(force_flush=True)
        self._check_and_report_debug_event()
        self._check_exception()
