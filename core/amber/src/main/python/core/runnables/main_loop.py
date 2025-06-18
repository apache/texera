# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import threading
import time
import typing
from loguru import logger
from overrides import overrides
from pampy import match
from typing import Iterator, Optional

from core.architecture.managers.context import Context
from core.architecture.managers.pause_manager import PauseType
from core.architecture.rpc.async_rpc_client import AsyncRPCClient
from core.architecture.rpc.async_rpc_server import AsyncRPCServer
from core.models import (
    InternalQueue,
    Tuple,
)
from core.models.internal_marker import StartChannel, EndChannel
from core.models.internal_queue import (
    DataElement,
    ControlElement,
    EmbeddedControlMessageElement,
    InternalQueueElement,
)
from core.models.state import State
from core.runnables.data_processor import DataProcessor
from core.util import StoppableQueueBlockingRunnable, get_one_of
from core.util.console_message.timestamp import current_time_in_local_timezone
from core.util.customized_queue.queue_base import QueueElement
from proto.edu.uci.ics.amber.engine.architecture.rpc import (
    ConsoleMessage,
    ControlInvocation,
    ConsoleMessageType,
    ReturnInvocation,
    PortCompletedRequest,
    EmptyRequest,
    ConsoleMessageTriggeredRequest,
    EmbeddedControlMessageType,
    EmbeddedControlMessage,
    AsyncRpcContext,
    ControlRequest,
)
from proto.edu.uci.ics.amber.engine.architecture.worker import (
    WorkerState,
)
from proto.edu.uci.ics.amber.engine.common import ControlPayloadV2
from proto.edu.uci.ics.amber.core import (
    ActorVirtualIdentity,
    PortIdentity,
    ChannelIdentity,
    EmbeddedControlMessageIdentity,
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
        self.context.statistics_manager.update_total_execution_time(time.time_ns())
        controller_interface = self._async_rpc_client.controller_stub()
        controller_interface.worker_execution_completed(EmptyRequest())
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
        self.context.statistics_manager.initialize_worker_start_time(time.time_ns())

    @overrides
    def receive(self, next_entry: QueueElement) -> None:
        """
        Main entry point of the DataProcessor. Upon receipt of an next_entry,
        process it respectfully.

        :param next_entry: An entry from input_queue, could be one of the followings:
                    1. a ControlElement;
                    2. a DataElement.
        """
        if isinstance(next_entry, InternalQueueElement):
            self.context.current_input_channel_id = next_entry.tag

        match(
            next_entry,
            DataElement,
            self._process_data_element,
            ControlElement,
            self._process_control_element,
            EmbeddedControlMessageElement,
            self._process_ecm,
        )

    def process_control_payload(
        self, tag: ChannelIdentity, payload: ControlPayloadV2
    ) -> None:
        """
        Process the given ControlPayload with the tag.

        :param tag: ChannelIdentity, the sender.
        :param payload: ControlPayloadV2 to be handled.
        """
        start_time = time.time_ns()
        match(
            (tag, get_one_of(payload, sealed=False)),
            typing.Tuple[ChannelIdentity, ControlInvocation],
            self._async_rpc_server.receive,
            typing.Tuple[ChannelIdentity, ReturnInvocation],
            self._async_rpc_client.receive,
        )
        end_time = time.time_ns()
        self.context.statistics_manager.increase_control_processing_time(
            end_time - start_time
        )
        self.context.statistics_manager.update_total_execution_time(end_time)

    def process_input_tuple(self) -> None:
        """
        Process the current input tuple with the current input link.
        Send all result Tuples or State to downstream workers.

        This is being invoked for each Tuple that are unpacked from the DataElement.
        """
        if isinstance(self.context.tuple_processing_manager.current_input_tuple, Tuple):
            self.context.statistics_manager.increase_input_statistics(
                self.context.tuple_processing_manager.current_input_port_id,
                self.context.tuple_processing_manager.current_input_tuple.in_mem_size(),
            )

        for output_tuple in self.process_tuple_with_udf():
            self._check_and_process_control()
            if output_tuple is not None:
                self.context.statistics_manager.increase_output_statistics(
                    PortIdentity(0), output_tuple.in_mem_size()
                )
                for to, batch in self.context.output_manager.tuple_to_batch(
                    output_tuple
                ):
                    self._output_queue.put(
                        DataElement(
                            tag=ChannelIdentity(
                                ActorVirtualIdentity(self.context.worker_id), to, False
                            ),
                            payload=batch,
                        )
                    )
                self.context.output_manager.save_tuple_to_storage_if_needed(
                    output_tuple
                )

    def process_input_state(self) -> None:
        self._switch_context()
        output_state = self.context.state_processing_manager.get_output_state()
        self._switch_context()
        if output_state is not None:
            for to, batch in self.context.output_manager.emit_state(output_state):
                self._output_queue.put(
                    DataElement(
                        tag=ChannelIdentity(
                            ActorVirtualIdentity(self.context.worker_id), to, False
                        ),
                        payload=batch,
                    )
                )

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

    def _process_tuple(self, tuple_: Tuple) -> None:
        self.context.tuple_processing_manager.current_input_tuple = tuple_
        self.process_input_tuple()
        self._check_and_process_control()

    def _process_state(self, state_: State) -> None:
        self.context.state_processing_manager.current_input_state = state_
        self.process_input_state()
        self._check_and_process_control()

    def _process_start_channel(self) -> None:
        self._send_ecm_to_data_channels(
            "StartChannel", EmbeddedControlMessageType.NO_ALIGNMENT
        )
        self.process_input_state()

    def _process_end_channel(self) -> None:
        self.process_input_state()
        self.process_input_tuple()

        input_port_id = self.context.input_manager.get_port_id(
            self.context.current_input_channel_id
        )

        if input_port_id is not None:
            self._async_rpc_client.controller_stub().port_completed(
                PortCompletedRequest(
                    port_id=input_port_id,
                    input=True,
                )
            )

        if self.context.input_manager.all_ports_completed():
            # Special case for the hack of input port dependency.
            # See documentation of is_missing_output_ports
            if self.context.output_manager.is_missing_output_ports():
                return
            self.context.output_manager.close_port_storage_writers()

            self._send_ecm_to_data_channels(
                "EndChannel", EmbeddedControlMessageType.PORT_ALIGNMENT
            )

            # Need to send port completed even if there is no downstream link
            for port_id in self.context.output_manager.get_port_ids():
                self._async_rpc_client.controller_stub().port_completed(
                    PortCompletedRequest(port_id=port_id, input=False)
                )
            self.complete()

    def _process_ecm(self, ecm_element: EmbeddedControlMessageElement):
        """
        Processes a received ECM and handles synchronization,
        command execution, and forwarding to downstream channels if applicable.

        Args:
            ecm_element (EmbeddedControlMessageElement): The received ECM element.
        """
        ecm = ecm_element.payload
        command = ecm.command_mapping.get(self.context.worker_id)
        channel_id = self.context.current_input_channel_id
        logger.info(
            f"receive channel ECM from {channel_id}," f" id = {ecm.id}, cmd = {command}"
        )
        if ecm.ecm_type != EmbeddedControlMessageType.NO_ALIGNMENT:
            self.context.pause_manager.pause_input_channel(
                PauseType.ECM_PAUSE, channel_id
            )

        if self.context.ecm_manager.is_ecm_aligned(channel_id, ecm):
            logger.info(
                f"process channel ECM from {channel_id},"
                f" id = {ecm.id}, cmd = {command}"
            )

            if command is not None:
                self._async_rpc_server.receive(channel_id, command)

            downstream_channels_in_scope = {
                scope
                for scope in ecm.scope
                if scope.from_worker_id == ActorVirtualIdentity(self.context.worker_id)
            }
            if downstream_channels_in_scope:
                for (
                    active_channel_id
                ) in self.context.output_manager.get_output_channel_ids():
                    if active_channel_id in downstream_channels_in_scope:
                        logger.info(
                            f"send ECM to {active_channel_id},"
                            f" id = {ecm.id}, cmd = {command}"
                        )
                        self._send_ecm_to_channel(active_channel_id, ecm)

            if ecm.ecm_type != EmbeddedControlMessageType.NO_ALIGNMENT:
                self.context.pause_manager.resume(PauseType.ECM_PAUSE)

            if self.context.tuple_processing_manager.current_internal_marker:
                {
                    StartChannel: self._process_start_channel,
                    EndChannel: self._process_end_channel,
                }[type(self.context.tuple_processing_manager.current_internal_marker)]()

    def _send_ecm_to_data_channels(
        self, method_name: str, alignment: EmbeddedControlMessageType
    ) -> None:
        for active_channel_id in self.context.output_manager.get_output_channel_ids():
            if not active_channel_id.is_control:
                ecm = EmbeddedControlMessage(
                    EmbeddedControlMessageIdentity(method_name),
                    alignment,
                    [],
                    {
                        active_channel_id.to_worker_id.name: ControlInvocation(
                            method_name,
                            ControlRequest(empty_request=EmptyRequest()),
                            AsyncRpcContext(
                                ActorVirtualIdentity(), ActorVirtualIdentity()
                            ),
                            -1,
                        )
                    },
                )
                self._send_ecm_to_channel(active_channel_id, ecm)

    def _send_ecm_to_channel(
        self, channel_id: ChannelIdentity, ecm: EmbeddedControlMessage
    ) -> None:
        for batch in self.context.output_manager.emit_ecm(channel_id.to_worker_id, ecm):
            tag = channel_id
            element = (
                EmbeddedControlMessageElement(tag=tag, payload=batch)
                if isinstance(batch, EmbeddedControlMessage)
                else DataElement(tag=tag, payload=batch)
            )
            self._output_queue.put(element)

    def _process_data_element(self, data_element: DataElement) -> None:
        """
        Upon receipt of a DataElement, unpack it into Tuples and States,
        and process them one by one.

        :param data_element: DataElement, a batch of data.
        """

        self.context.tuple_processing_manager.current_input_port_id = (
            self.context.input_manager.get_port_id(
                self.context.current_input_channel_id
            )
        )

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
        # here the self.context.processing_manager.current_input_iter
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
                    State,
                    self._process_state,
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

    def _send_console_message(self, console_message: ConsoleMessage):

        self._async_rpc_client.controller_stub().console_message_triggered(
            ConsoleMessageTriggeredRequest(console_message=console_message)
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
        self.context.statistics_manager.update_total_execution_time(end_time)

    def _check_and_report_debug_event(self) -> None:
        if self.context.debug_manager.has_debug_event():
            debug_event = self.context.debug_manager.get_debug_event()
            self._send_console_message(
                ConsoleMessage(
                    worker_id=self.context.worker_id,
                    timestamp=current_time_in_local_timezone(),
                    msg_type=ConsoleMessageType.DEBUGGER,
                    source="(Pdb)",
                    title=debug_event,
                    message="",
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
        self._check_and_report_console_messages(force_flush=True)
        self._check_and_report_debug_event()
        self._check_exception()
