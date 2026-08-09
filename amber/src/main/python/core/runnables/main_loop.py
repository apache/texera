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
    StateFrame,
    Tuple,
)
from core.models.internal_marker import StartChannel, EndChannel
from core.models.internal_queue import (
    DataElement,
    DCMElement,
    ECMElement,
    InternalQueueElement,
)
from core.models.operator import LoopEndOperator, LoopStartOperator
from core.models.state import State
from core.runnables.data_processor import DataProcessor
from core.storage.document_factory import DocumentFactory
from core.util import StoppableQueueBlockingRunnable, get_one_of
from core.util.console_message.timestamp import current_time_in_local_timezone
from core.util.customized_queue.queue_base import QueueElement
from core.util.virtual_identity import get_logical_op_id, get_physical_op_id_string
from proto.org.apache.texera.amber.core import (
    ActorVirtualIdentity,
    PortIdentity,
    ChannelIdentity,
    EmbeddedControlMessageIdentity,
    OperatorIdentity,
)
from proto.org.apache.texera.amber.engine.architecture.rpc import (
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
    JumpToOperatorRegionRequest,
)
from proto.org.apache.texera.amber.engine.architecture.worker import (
    WorkerState,
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
        # Captured from the consumed StateFrame envelope when a matching
        # LoopEnd (loop_counter == 0) takes a state; used for the jump RPC
        # and the setup-config URI lookup (context.loop_start_state_uris).
        self._loop_start_id: str = ""
        # Whether this LoopEnd already consumed its loop state this execution.
        # A loop body may branch and converge on the Loop End, and every reader
        # on its input port replays that branch's states independently, so the
        # same iteration's state arrives once per branch. Workers are recreated
        # on each region re-execution, so this instance flag is per iteration.
        self._loop_state_consumed: bool = False
        # Per-port drain contagion, mirroring the Scala worker: a port is
        # poisoned when an `__error__` State arrives on
        # it; `_self_failed` poisons every port at once when this worker's own
        # executor throws. Data on a poisoned port is discarded without invoking
        # the executor and its finish hooks are suppressed -- so no
        # post-failure side effects run in user code -- while ports still
        # complete normally so the stream terminates instead of hanging.
        self._poisoned_ports: typing.Set[PortIdentity] = set()
        self._self_failed: bool = False

        self.context = Context(worker_id, input_queue)
        self._async_rpc_server = AsyncRPCServer(output_queue, context=self.context)
        self._async_rpc_client = AsyncRPCClient(output_queue, context=self.context)

        self.data_processor = DataProcessor(self.context)
        threading.Thread(
            target=self.data_processor.run, daemon=True, name="data_processor_thread"
        ).start()

    def _jump_to_loop_start(
        self, executor: LoopEndOperator, coordinator_interface
    ) -> None:
        # The write address is setup config, keyed by the captured id. Fail
        # loud BEFORE the jump RPC so a misconfigured loop does not rewind the
        # schedule without a back-edge write. Anything raised here (a missing
        # URI, or a failed state write after the jump) is reported by
        # complete()'s guard as an operator-facing error.
        uri = self.context.loop_start_state_uris.get(self._loop_start_id)
        if not uri:
            raise RuntimeError(
                f"no loop-back state URI configured for LoopStart "
                f"'{self._loop_start_id}' "
                f"(have: {sorted(self.context.loop_start_state_uris)})"
            )
        coordinator_interface.jump_to_operator_region(
            JumpToOperatorRegionRequest(OperatorIdentity(self._loop_start_id))
        )
        writer = DocumentFactory.create_document(uri, State.SCHEMA).writer("0")
        # The back-edge fires only after the matching LoopEnd consumed at
        # loop_counter == 0, so the next iteration's input starts at depth 0.
        writer.put_one(executor.state.to_tuple(0))
        writer.close()

    def complete(self) -> None:
        """
        Complete the DataProcessor, marking state to COMPLETED, and notify the
        coordinator.
        """
        # flush the buffered console prints
        self._check_and_report_console_messages(force_flush=True)
        coordinator_interface = self._async_rpc_client.coordinator_stub()
        executor = self.context.executor_manager.executor
        if isinstance(executor, LoopEndOperator) and self._self_failed:
            # This LoopEnd's own executor failed: do not evaluate condition() or
            # take the back-edge -- a failed iteration must not re-iterate on
            # partial loop state. The error State already went downstream, so an
            # enclosing try/catch frame (or the console error) reports it; the
            # worker still completes so the stream terminates.
            pass
        elif isinstance(executor, LoopEndOperator):
            # condition() evaluates a user-supplied expression, and the
            # loop-back edge writes state to iceberg after the jump DCM --
            # both on this main loop thread, outside DataProcessor's guarded
            # executor session. A UDF error on the data path is caught and
            # reported via Context.report_exception
            # (DataProcessor._executor_session); reuse it here so a bad
            # condition (a typo, an undefined name) or a failed back-edge
            # write surfaces as an operator-facing error and pauses the
            # worker, instead of killing the thread through run()'s
            # @logger.catch(reraise=True).
            try:
                if executor.condition():
                    self._jump_to_loop_start(executor, coordinator_interface)
            except Exception as err:
                self.context.report_exception(err)
                self._check_exception()
                return
        executor.close()
        # stop the data processing thread
        self.data_processor.stop()
        self.context.state_manager.transit_to(WorkerState.COMPLETED)
        self.context.statistics_manager.update_total_execution_time(time.time_ns())
        coordinator_interface.worker_execution_completed(EmptyRequest())
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
            match(
                next_entry,
                DCMElement,
                self._process_dcm,
                ECMElement,
                self._process_ecm,
            )

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
            DCMElement,
            self._process_dcm,
            ECMElement,
            self._process_ecm,
        )

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
                self._emit_batches(
                    self.context.output_manager.tuple_to_batch(output_tuple)
                )
                self.context.output_manager.save_tuple_to_storage_if_needed(
                    output_tuple
                )

    def process_input_state(
        self,
        output_loop_counter: int = 0,
        output_loop_start_id: str = "",
    ) -> None:
        self._switch_context()
        output_state = self.context.state_processing_manager.get_output_state()
        if output_state is not None:
            executor = self.context.executor_manager.executor
            if isinstance(executor, LoopStartOperator):
                # A LoopStart stamps its own logical op id; the write address
                # is setup config (InitializeExecutorRequest.loop_start_state_uris).
                output_loop_start_id = get_logical_op_id(self.context.worker_id)
            self._emit_and_save_state(
                output_state,
                output_loop_counter,
                output_loop_start_id,
            )

    def _emit_batches(self, batches) -> None:
        """Put each (receiver, batch) pair on the output queue as a DataElement."""
        for to, batch in batches:
            self._output_queue.put(
                DataElement(
                    tag=ChannelIdentity(
                        ActorVirtualIdentity(self.context.worker_id), to, False
                    ),
                    payload=batch,
                )
            )

    def _emit_and_save_state(
        self,
        state: State,
        loop_counter: int,
        loop_start_id: str = "",
    ) -> None:
        # State serialization (state.to_tuple -> to_json) and the storage write
        # run here on the main loop thread, outside DataProcessor's guarded
        # executor session. A non-serializable loop variable (e.g. a numpy
        # array) would otherwise raise a TypeError that propagates through
        # run()'s @logger.catch(reraise=True), killing the thread and hanging
        # the workflow with no operator-facing error. Report it like a UDF error
        # (exception manager + ERROR console message + EXCEPTION_PAUSE) instead;
        # callers on the end-channel path check has_exception and hold the
        # region so the reported error is not a silent, false success.
        try:
            self._emit_batches(
                self.context.output_manager.emit_state(
                    state, loop_counter, loop_start_id
                )
            )
            self.context.output_manager.save_state_to_storage_if_needed(
                state, loop_counter, loop_start_id
            )
        except Exception as err:
            self.context.report_exception(err)
            self._check_exception()

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

    def _process_dcm(self, dcm_element: DCMElement) -> None:
        """
        Upon receipt of a ControlElement, unpack it into tag and payload to be handled.

        :param dcm_element: DirectControlMessageElement to be handled.
        """
        start_time = time.time_ns()
        match(
            (dcm_element.tag, get_one_of(dcm_element.payload, sealed=False)),
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

    def _process_tuple(self, tuple_: Tuple) -> None:
        port_id = self.context.tuple_processing_manager.current_input_port_id
        if self._is_port_poisoned(port_id):
            # This data belongs to an already-failed attempt: consume and
            # discard without invoking the executor (no post-failure side
            # effects in user code). No control check here -- unlike real
            # processing, a discard is O(1), and _check_and_process_control
            # blocks while data is disabled; control messages are still handled
            # between batches and on markers.
            if port_id is not None:
                self.context.statistics_manager.increase_input_statistics(
                    port_id, tuple_.in_mem_size()
                )
            return
        self.context.tuple_processing_manager.current_input_tuple = tuple_
        self.process_input_tuple()
        self._check_and_process_control()

    def _process_state_frame(self, frame: StateFrame) -> None:
        # The runtime owns loop_counter; loop operators never see or mutate it.
        # The LoopStart/LoopEnd nested pass-through branches are handled here --
        # forwarding the state and skipping the operator -- so the operator's
        # process_state only ever runs the first-entry / consume path.
        state = frame.frame
        in_counter = frame.loop_counter
        executor = self.context.executor_manager.executor

        if isinstance(executor, LoopEndOperator) and in_counter > 0:
            # An inner Loop End receiving the enclosing (outer) loop's boundary
            # state (loop_counter > 0): the signal that the outer loop has
            # advanced. Reset this Loop End's output now, before forwarding, so
            # the new outer iteration's inner results accumulate from empty
            # instead of concatenating across outer iterations. The input reader
            # replays all states before any data each region execution, so the
            # result/state tables still hold the PREVIOUS outer iteration's rows
            # at this point. This fires exactly once per outer iteration: the
            # inner LoopStart's output (and thus this pass-through) is recreated
            # on every inner back-edge, so the outer state only reaches here on
            # the first inner iteration of each outer iteration. A single /
            # outermost Loop End never reaches this branch (no enclosing loop,
            # so never loop_counter > 0) and so never resets -- it accumulates
            # all of its own iterations.
            self.context.output_manager.reset_output_storage()
            # State belongs to an outer loop: step one level out and forward,
            # carrying the outer loop's id unchanged.
            self._emit_and_save_state(state, in_counter - 1, frame.loop_start_id)
            self._check_and_process_control()
            return
        if isinstance(executor, LoopStartOperator) and frame.loop_start_id:
            # Outer loop's state flowing through an inner LoopStart -- detected
            # by the outer LoopStart's id stamped on the envelope (a first-entry
            # state has no stamp): step one level deeper and forward, keeping
            # the outer loop's id.
            self._emit_and_save_state(state, in_counter + 1, frame.loop_start_id)
            self._check_and_process_control()
            return

        if isinstance(executor, LoopEndOperator):
            # Matching LoopEnd (in_counter == 0): it will consume this state
            # and jump back. Remember which LoopStart to jump to (it rides
            # the envelope) for complete()/_jump_to_loop_start.
            #
            # With a branching loop body, each branch's reader replays the same
            # iteration's state, so this fires once per inbound link. Consume
            # only the first: running the user's `update` again would advance
            # the loop variables once per branch. The copies are identical --
            # they are the same state emitted by the one matching LoopStart --
            # so dropping them loses nothing (a consume emits nothing
            # downstream either way).
            if self._loop_state_consumed:
                self._check_and_process_control()
                return
            self._loop_state_consumed = True
            self._loop_start_id = frame.loop_start_id

        self.context.state_processing_manager.current_input_state = state
        self.process_input_state(
            output_loop_counter=in_counter,
            output_loop_start_id=frame.loop_start_id,
        )
        # Poison AFTER delivery: the executor sees the error State first (frame
        # operators react to it; the default pass-through forwards it), then the
        # port drains from the next data tuple on.
        if isinstance(state, State) and state.is_error():
            port_id = self._current_input_port_id()
            if port_id is not None:
                self._poisoned_ports.add(port_id)
        self._check_and_process_control()

    def _process_start_channel(self) -> None:
        self._send_ecm_to_data_channels(
            "StartChannel", EmbeddedControlMessageType.NO_ALIGNMENT
        )
        self.process_input_state()

    def _process_end_channel(self) -> None:
        input_port_id = self._current_input_port_id()
        # A poisoned port belongs to a failed attempt: suppress the executor's
        # finish hooks (produce_state_on_finish / on_finish) so no post-failure
        # side effects or junk final emissions happen. Port completion below
        # still runs, so the stream terminates normally instead of hanging.
        if not self._is_port_poisoned(input_port_id):
            self.process_input_state()
            self.process_input_tuple()

        if input_port_id is not None:
            self._async_rpc_client.coordinator_stub().port_completed(
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
                self._async_rpc_client.coordinator_stub().port_completed(
                    PortCompletedRequest(port_id=port_id, input=False)
                )
            self.complete()

    def _process_ecm(self, ecm_element: ECMElement):
        """
        Processes a received ECM and handles synchronization,
        command execution, and forwarding to downstream channels if applicable.

        Args:
            ecm_element (ECMElement): The received ECM element.
        """
        ecm = ecm_element.payload
        command = ecm.command_mapping.get(self.context.worker_id)
        channel_id = self.context.current_input_channel_id
        logger.debug(
            "receive channel ECM from {}, id = {}, cmd = {}",
            channel_id,
            ecm.id,
            command,
        )
        if ecm.ecm_type != EmbeddedControlMessageType.NO_ALIGNMENT:
            self.context.pause_manager.pause_input_channel(
                PauseType.ECM_PAUSE, channel_id
            )

        if self.context.ecm_manager.is_ecm_aligned(channel_id, ecm):
            logger.debug(
                "process channel ECM from {}, id = {}, cmd = {}",
                channel_id,
                ecm.id,
                command,
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
                        logger.debug(
                            "send ECM to {}, id = {}, cmd = {}",
                            active_channel_id,
                            ecm.id,
                            command,
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
                ECMElement(tag=tag, payload=batch)
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
                    StateFrame,
                    self._process_state_frame,
                )
            except Exception as err:
                logger.exception(err)

    def _send_console_message(self, console_message: ConsoleMessage):
        self._async_rpc_client.coordinator_stub().console_message_triggered(
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
        """Route a reported operator exception down the failure path.

        Mirror of the Scala worker's `handleExecutorException`: the console
        message always reports the error first. Then, GUARDED workers (inside
        a try/catch frame's cone) broadcast an
        `__error__` State so downstream workers drain and the frame can react,
        and drain their own remaining input — ports still complete, the run
        terminates. UNGUARDED workers keep the default product behavior:
        EXCEPTION_PAUSE, leaving the worker inspectable and the current input
        retriable (RetryCurrentTuple).
        """
        if self.context.exception_manager.has_exception():
            self._check_and_report_console_messages(force_flush=True)
            if not self.context.guarded:
                self.context.pause_manager.pause(PauseType.EXCEPTION_PAUSE)
            elif not self._self_failed:
                self._self_failed = True
                self._emit_error_state()

    def _emit_error_state(self) -> None:
        """Broadcast the error State for this worker's own failure."""
        # Read the field directly: get_exc_info() CONSUMES the exception (it
        # nulls exc_info for the replay/debug path), and clearing it here would
        # make the worker look healthy to every later has_exception() check.
        exc_info = self.context.exception_manager.exc_info
        err = exc_info[1] if exc_info else RuntimeError("operator failed")
        try:
            operator_id = get_physical_op_id_string(self.context.worker_id)
        except ValueError:
            # A malformed worker id must not raise on the failure path -- a
            # secondary error here would lose the signal entirely. Fall back to
            # the raw id: frame attribution then treats the error as foreign,
            # which escalates outward (the safe direction) instead of a frame
            # wrongly claiming it.
            operator_id = self.context.worker_id
        error_state = State.error(
            operator_id,
            self.context.worker_id,
            err,
        )
        # Emit directly rather than via _emit_and_save_state: that helper
        # funnels its own failures back into _check_exception, which is the
        # caller of this method. Both halves of the Scala worker's emitState
        # are needed: the network buffers for live links, AND the port state
        # storage for MATERIALIZED links — a try-cone tail's outgoing edges
        # (a Finally's dependee `From Try`, a gate's dependee signal ports)
        # are materialized and have no live partitioners at all, so storage
        # is the only road the failure signal can travel to the gate.
        try:
            self._emit_batches(self.context.output_manager.emit_state(error_state))
            self.context.output_manager.save_state_to_storage_if_needed(error_state)
        except Exception as emit_err:  # pragma: no cover - defensive
            logger.exception(emit_err)

    def _current_input_port_id(self) -> Optional[PortIdentity]:
        """The input port the current channel belongs to, or None when the
        channel is not registered (only reachable outside a normal data path)."""
        channel_id = self.context.current_input_channel_id
        if channel_id is None:
            return None
        try:
            return self.context.input_manager.get_port_id(channel_id)
        except KeyError:
            return None

    def _is_port_poisoned(self, port_id: Optional[PortIdentity]) -> bool:
        """Whether this port belongs to an already-failed attempt: data on it is
        discarded without invoking the executor, and the executor's finish hooks
        for it are suppressed (per-port drain contagion)."""
        return self._self_failed or (
            port_id is not None and port_id in self._poisoned_ports
        )

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
