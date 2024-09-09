import datetime
import threading
import traceback
import typing
from typing import Iterator, Optional, Union

from loguru import logger
from overrides import overrides
from pampy import match

from core.architecture.managers.context import Context
from core.architecture.managers.pause_manager import PauseType
from core.architecture.packaging.batch_to_tuple_converter import EndOfAllMarker
from core.architecture.rpc.async_rpc_client import AsyncRPCClient
from core.architecture.rpc.async_rpc_server import AsyncRPCServer
from core.models import (
    InputExhausted,
    InternalQueue,
    SenderChangeMarker,
    Tuple,
    ExceptionInfo,
)
from core.models.internal_queue import DataElement, ControlElement
from core.runnables.data_processor import DataProcessor
from core.util import StoppableQueueBlockingRunnable, get_one_of, set_one_of
from core.util.customized_queue.queue_base import QueueElement
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    ControlCommandV2,
    LocalOperatorExceptionV2,
    WorkerExecutionCompletedV2,
    WorkerState,
    LinkCompletedV2,
    PythonConsoleMessageV2,
    WorkerDebugCommandV2,
    StateRequestV2,
    StateReturn,
)
from proto.edu.uci.ics.amber.engine.common import (
    ActorVirtualIdentity,
    ControlInvocationV2,
    ControlPayloadV2,
    ReturnInvocationV2,
)
import time


class MainLoop(StoppableQueueBlockingRunnable):
    def __init__(
        self, worker_id: str, input_queue: InternalQueue, output_queue: InternalQueue
    ):
        super().__init__(self.__class__.__name__, queue=input_queue)
        self._input_queue: InternalQueue = input_queue
        self._output_queue: InternalQueue = output_queue

        self.context = Context(worker_id, self)
        self._async_rpc_server = AsyncRPCServer(output_queue, context=self.context)
        self._async_rpc_client = AsyncRPCClient(output_queue, context=self.context)

        self.data_processor = DataProcessor(self.context)
        threading.Thread(
            target=self.data_processor.run, daemon=True, name="data_processor_thread"
        ).start()
        self.main_loop_start_time = time.time()

    def complete(self) -> None:
        """
        Complete the DataProcessor, marking state to COMPLETED, and notify the
        controller.
        """
        # flush the buffered console prints
        self._check_and_report_print(force_flush=True)
        self.context.operator_manager.operator.close()
        # stop the data processing thread
        self.data_processor.stop()
        self.context.state_manager.transit_to(WorkerState.COMPLETED)
        control_command = set_one_of(ControlCommandV2, WorkerExecutionCompletedV2())
        self._async_rpc_client.send(
            ActorVirtualIdentity(name="CONTROLLER"), control_command
        )
        self.context.close()
        import bdb

        logger.info("total time in eval: " + str(bdb.total_time))
        logger.info(
            "total time of operator: " + str(time.time() - self.main_loop_start_time)
        )

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
            or self.context.pause_manager.is_paused()
        ):
            next_entry = self.interruptible_get()
            self._process_control_element(next_entry)

    @overrides
    def pre_start(self) -> None:
        self.context.state_manager.assert_state(WorkerState.UNINITIALIZED)
        self.context.state_manager.transit_to(WorkerState.READY)

    @overrides
    def receive(self, next_entry: QueueElement) -> None:
        """
        Main entry point of the DataProcessor. Upon receipt of an next_entry,
        process it respectfully.

        :param next_entry: An entry from input_queue, could be one of the followings:
                    1. a ControlElement;
                    2. a DataElement.
        """
        if isinstance(
            next_entry, DataElement
        ) and self.context.state_manager.confirm_state(WorkerState.READY):
            # add simulated debug comments here - for experimental purpose

            # examples:

            # add a breakpoint
            # self.simulate_debug_command("b linenum[, condition]")

            # change from ["W1", "W2", "W3", "W4", "W5", "W6Passive", "W6Active"]
            test = "W4"

            if test == "W5":
                # For W1:
                self.simulate_debug_command(
                    "b 50, 'Udon' in tuple_['text'] and 'welcome' in tokens"
                )

            elif test == "W2":
                # For W2:
                self.simulate_debug_command(
                    "b 26, 'Udon' in tuple_['text'] and 'welcome' in tokens"
                )

            elif test == "W3":
                # For W3:
                self.simulate_debug_command(
                    "b 9, tuple_['column-5'] > 10"
                )

            elif test == "W4":
                # For W4:
                if "fc88c19f-1f8a-4c7e-8578-8272952c47c8" not in self.context.worker_id: # only break for Image Rotator
                    self.simulate_debug_command(
                        "b 18, H > 400"
                    )

            elif test == "W5":
                # For W5:
                self.simulate_debug_command(
                    "b 23, sum(1 for token in doc if token.pos_ == 'NOUN') > 10"
                )

            elif "W6" in test:
                # For state transfer between multiple operators
                upstream = "PythonUDFV2-operator-21e8857b-8c1b-4cc0-9b88-03026d791e2a-main-0"
                downstream = "PythonUDFV2-operator-e6e9797a-fe9d-444a-bc43-1f28f42b9f03-main-0"

                if "Passive" in test:
                    # For W6 passive transfer (Store state + Request State)

                    if upstream in self.context.worker_id:
                        self.simulate_debug_command(
                            "ss 51 tokens"
                        )
                    if downstream in self.context.worker_id:
                        self.simulate_debug_command(
                            f"rs 17 51 tokens {self.context.worker_id.split('-')[0]}-{upstream}"
                        )
                else:
                    #For W6 active transfer (Append State):
                    if upstream in self.context.worker_id:
                        self.simulate_debug_command(
                            "as 51 tokens"
                        )
            pass

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
        match(
            (tag, get_one_of(payload)),
            typing.Tuple[ActorVirtualIdentity, ControlInvocationV2],
            self._async_rpc_server.receive,
            typing.Tuple[ActorVirtualIdentity, ReturnInvocationV2],
            self._async_rpc_client.receive,
        )

    def process_input_tuple(self) -> None:
        """
        Process the current input tuple with the current input link. Send all result
        Tuples to downstream operators.

        This is being invoked for each Tuple/Marker that are unpacked from the
        DataElement.
        """
        if isinstance(self.context.tuple_processing_manager.current_input, Tuple):
            self.context.statistics_manager.increase_input_tuple_count()
        self.context.operator_manager.apply_available_code_update()
        for output_tuple in self.process_tuple_with_udf():
            self._check_and_process_control()
            if output_tuple is not None:
                self.context.statistics_manager.increase_output_tuple_count()
                for (
                    to,
                    batch,
                ) in self.context.tuple_to_batch_converter.tuple_to_batch(output_tuple):
                    batch.schema = self.context.operator_manager.operator.output_schema
                    self._output_queue.put(DataElement(tag=to, payload=batch))

    def process_tuple_with_udf(self) -> Iterator[Optional[Tuple]]:
        """
        Process the Tuple/InputExhausted with the current link.

        This is a wrapper to invoke processing of the operator.

        :return: Iterator[Tuple], iterator of result Tuple(s).
        """
        finished_current = self.context.tuple_processing_manager.finished_current
        finished_current.clear()

        while not finished_current.is_set():
            self._check_and_process_control()
            self._switch_context()
            output = self.context.tuple_processing_manager.get_output()
            if isinstance(output, str):
                self.handle_state_transfer_statements(output)
            else:
                yield output

    def handle_state_return(self, state_name, state_return: StateReturn):
        import pickle

        self.context.tuple_processing_manager.output_iterator.gi_frame.f_locals[
            state_name
        ] = pickle.loads(state_return.bytes)
        self._resume_dp()

    def report_exception(self, exc_info: ExceptionInfo) -> None:
        """
        Report the traceback of current stack when an exception occurs.
        """
        message: str = "\n".join(traceback.format_exception(*exc_info))
        control_command = set_one_of(
            ControlCommandV2, LocalOperatorExceptionV2(message=message)
        )
        self._async_rpc_client.send(
            ActorVirtualIdentity(name="CONTROLLER"), control_command
        )

    def _process_control_element(self, control_element: ControlElement) -> None:
        """
        Upon receipt of a ControlElement, unpack it into tag and payload to be handled.

        :param control_element: ControlElement to be handled.
        """
        self.process_control_payload(control_element.tag, control_element.payload)

    def _process_tuple(self, tuple_: Union[Tuple, InputExhausted]) -> None:
        self.context.tuple_processing_manager.current_input = tuple_
        self.process_input_tuple()
        self._check_and_process_control()

    def _process_input_exhausted(self, input_exhausted: InputExhausted):
        self._process_tuple(input_exhausted)
        if self.context.tuple_processing_manager.current_input_link is not None:
            control_command = set_one_of(
                ControlCommandV2,
                LinkCompletedV2(
                    self.context.tuple_processing_manager.current_input_link
                ),
            )
            self._async_rpc_client.send(
                ActorVirtualIdentity(name="CONTROLLER"), control_command
            )

    def _process_sender_change_marker(
        self, sender_change_marker: SenderChangeMarker
    ) -> None:
        """
        Upon receipt of a SenderChangeMarker, change the current input link to the
        sender.

        :param sender_change_marker: SenderChangeMarker which contains sender link.
        """
        self.context.tuple_processing_manager.current_input_link = (
            sender_change_marker.link
        )

    def _process_end_of_all_marker(self, _: EndOfAllMarker) -> None:
        """
        Upon receipt of an EndOfAllMarker, which indicates the end of all input links,
        send the last data batches to all downstream workers.

        It will also invoke complete() of this DataProcessor.

        :param _: EndOfAllMarker
        """
        for to, batch in self.context.tuple_to_batch_converter.emit_end_of_upstream():
            batch.schema = self.context.operator_manager.operator.output_schema
            self._output_queue.put(DataElement(tag=to, payload=batch))
            self._check_and_process_control()
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
            self.context.batch_to_tuple_converter.process_data_payload(
                data_element.tag, data_element.payload
            )
        )

        if self.context.tuple_processing_manager.current_input_tuple_iter is None:
            return
        # here the self.context.processing_manager.current_input_tuple_iter
        # could be modified during iteration, thus we are using the try-while-stop_
        # iteration way to iterate through the iterator, instead of the for-each-loop
        # syntax sugar.
        while True:
            # In Python@3.8 there is a new `:=` operator to simplify this assignment
            # in while-loop. For now we keep it this way to support versions below
            # 3.8.
            try:
                element = next(
                    self.context.tuple_processing_manager.current_input_tuple_iter
                )
            except StopIteration:
                # StopIteration is the standard way for an iterator to end, we handle
                # it and terminate the loop.
                break
            try:
                match(
                    element,
                    Tuple,
                    self._process_tuple,
                    InputExhausted,
                    self._process_input_exhausted,
                    SenderChangeMarker,
                    self._process_sender_change_marker,
                    EndOfAllMarker,
                    self._process_end_of_all_marker,
                )
            except Exception as err:
                logger.exception(err)

    def _scheduler_time_slot_event(self, time_slot_expired: bool) -> None:
        """
        The time slot for scheduling this worker has expired.
        """
        if time_slot_expired:
            self.context.pause_manager.record_request(
                PauseType.SCHEDULER_TIME_SLOT_EXPIRED_PAUSE, True
            )
            self._input_queue.disable_data()
        else:
            self.context.pause_manager.record_request(
                PauseType.SCHEDULER_TIME_SLOT_EXPIRED_PAUSE, False
            )
            if not self.context.pause_manager.is_paused():
                self.context.input_queue.enable_data()

    def _pause_dp(self) -> None:
        """
        Pause the data processing.
        """
        self._check_and_report_print(force_flush=True)
        if self.context.state_manager.confirm_state(
            WorkerState.RUNNING, WorkerState.READY
        ):
            self.context.pause_manager.record_request(PauseType.USER_PAUSE, True)
            self._input_queue.disable_data()
            self.context.state_manager.transit_to(WorkerState.PAUSED)

    def _resume_dp(self) -> None:
        """
        Resume the data processing.
        """
        if self.context.state_manager.confirm_state(WorkerState.PAUSED):
            self.context.pause_manager.record_request(PauseType.USER_PAUSE, False)
            if not self.context.pause_manager.is_paused():
                self.context.input_queue.enable_data()
            self.context.state_manager.transit_to(WorkerState.RUNNING)

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
        with self.context.tuple_processing_manager.context_switch_condition:
            self.context.tuple_processing_manager.context_switch_condition.notify()
            self.context.tuple_processing_manager.context_switch_condition.wait()
        self._post_switch_context_checks()

    def _check_and_report_debug_event(self) -> None:
        if self.context.debug_manager.has_debug_event():
            debug_event = self.context.debug_manager.get_debug_event()

            self._send_console_message(
                PythonConsoleMessageV2(
                    timestamp=datetime.datetime.now(),
                    msg_type="DEBUGGER",
                    source="(Pdb)",
                    message=debug_event,
                )
            )
            auto_resume = False

            if self.context.debug_manager.is_breakpoint_hit_event(debug_event):
                if not self.context.debug_manager.PAUSE_ON_HITTING_BREAKPOINT:
                    auto_resume = True

            if self.context.debug_manager.is_setting_breakpoint_event(debug_event):
                if not self.context.debug_manager.PAUSE_ON_SETTING_BREAKPOINT:
                    auto_resume = True

            if auto_resume:
                control_command = set_one_of(
                    ControlCommandV2,
                    WorkerDebugCommandV2(cmd="c"),
                )
                self._async_rpc_client.send(
                    ActorVirtualIdentity(name="SELF"), control_command
                )

            self._pause_dp()
            self._check_and_process_control()
            self._switch_context()

    def _check_and_report_exception(self) -> None:
        if self.context.exception_manager.has_exception():
            self.report_exception(self.context.exception_manager.get_exc_info())
            self._pause_dp()

    def _check_and_report_print(self, force_flush=False) -> None:
        for msg in self.context.console_message_manager.get_messages(force_flush):
            self._send_console_message(msg)

    def _post_switch_context_checks(self) -> None:
        """
        Post callback for switch context.

        One step in DataProcessor could produce some results, which includes
            - print messages
            - Debug Event
            - Exception
        We check and report them each time coming back from DataProcessor.
        """
        self._check_and_report_print()
        self._check_and_report_debug_event()
        self._check_and_report_exception()

    def simulate_debug_command(self, debug_command: str):
        control_command = set_one_of(
            ControlCommandV2,
            WorkerDebugCommandV2(cmd=debug_command),
        )
        self._async_rpc_client.send(ActorVirtualIdentity(name="SELF"), control_command)
        self._pause_dp()
        self._check_and_process_control()
        self._switch_context()

    def handle_state_transfer_statements(self, statement: str):
        if "request(" in statement:
            # request state
            target_worker_id, line_no, state_name = (
                statement.strip("request(").strip(")").split(",")
            )

            control_command = set_one_of(
                ControlCommandV2,
                StateRequestV2(
                    tuple_id=str(
                        self.context.tuple_processing_manager.current_input["id"]
                    ),
                    line_no=int(line_no),
                    state_name=state_name.strip(),
                ),
            )
            self._async_rpc_client.send(
                ActorVirtualIdentity(name=target_worker_id), control_command
            ).add_done_callback(
                lambda ret: self.handle_state_return(
                    state_name, ret.result().state_return
                )
            )
            logger.info("sent a request to " + target_worker_id)
            self._pause_dp()
            self._check_and_process_control()

        elif "store(" in statement:
            # store state
            lineno, state_name = statement.strip("store(").strip(")").split(",")
            state_name = state_name.strip()

            tuple_id = str(self.context.tuple_processing_manager.current_input["id"])
            self.context.debug_manager.states[
                (tuple_id, lineno, state_name)
            ] = self.context.tuple_processing_manager.output_iterator.gi_frame.f_locals[
                state_name
            ]
