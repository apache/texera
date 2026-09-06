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
    Table,
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
from core.storage.vfs_uri_factory import VFSURIFactory
from core.util import StoppableQueueBlockingRunnable, get_one_of
from core.util.console_message.replace_print import replace_print
from core.util.console_message.timestamp import current_time_in_local_timezone
from core.util.customized_queue.queue_base import QueueElement
from core.util.virtual_identity import get_logical_op_id
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
        # and the setup-config URI lookup (context.loop_start_port_uris).
        self._loop_start_id: str = ""
        # Whether this LoopEnd already took its loop state this execution.
        # A loop body may branch and converge on the Loop End, and every reader
        # on its input port replays that branch's states independently, so the
        # same iteration's state arrives once per branch. Cleared when the
        # deferred consume takes the stash (_consume_pending_loop_state), so
        # the flag is per-execution by construction -- not by the scheduler
        # happening to recreate workers each iteration.
        self._loop_state_consumed: bool = False
        # Whether this LoopEnd forwarded an UNstamped counter-0 state instead
        # of consuming it. Paired with _loop_start_id by
        # _check_loop_state_arrived: forwarding one and never capturing a stamp
        # means the loop's own state reached here with its stamp lost.
        self._forwarded_unstamped_state: bool = False
        # The matching state a LoopEnd will consume, stashed when taken and
        # actually handed to the operator at EndChannel (see
        # _consume_pending_loop_state for why the table read must not overlap
        # this worker's own materialization reader).
        self._pending_loop_state: Optional[State] = None

        self.context = Context(worker_id, input_queue)
        self._async_rpc_server = AsyncRPCServer(output_queue, context=self.context)
        self._async_rpc_client = AsyncRPCClient(output_queue, context=self.context)

        self.data_processor = DataProcessor(self.context)
        threading.Thread(
            target=self.data_processor.run, daemon=True, name="data_processor_thread"
        ).start()

    def _loop_start_base_uri(self) -> str:
        # The loop's bookkeeping base URI is setup config, keyed by the
        # captured id (see InitializeExecutorRequest.loopStartPortUris). Fail
        # loud on a missing entry: anything raised here is reported by the
        # caller's guard as an operator-facing error.
        uri = self.context.loop_start_port_uris.get(self._loop_start_id)
        if not uri:
            raise RuntimeError(
                f"no loop bookkeeping URI configured for LoopStart "
                f"'{self._loop_start_id}' "
                f"(have: {sorted(self.context.loop_start_port_uris)})"
            )
        return uri

    def _read_loop_input_table(self) -> Table:
        # The loop's input table is the Loop Start's input-port
        # materialization: the output doc of whatever feeds this loop level.
        # It is stable for the duration of the loop level that reads it -- the
        # back-edge rewrites only the state doc under the same base URI, never
        # this result doc. For an INNER loop that upstream is the outer Loop
        # Start, whose output doc is recreated on each OUTER iteration; that is
        # still stable across every inner iteration that reads it, which is the
        # window that matters here. Reading it means the table never has to
        # ride inside the State content through the loop body. Callers must
        # invoke this OUTSIDE the window where this worker's own materialization
        # reader is streaming -- see _consume_pending_loop_state.
        result_uri = VFSURIFactory.result_uri(self._loop_start_base_uri())
        document, schema = DocumentFactory.open_document(result_uri)
        # Normalize the same way as the only other reader of this doc: the
        # input-port reader casts every tuple to the doc's schema before it
        # reaches an operator (input_port_materialization_reader_runnable), so
        # this read must too -- two readers of one document normalizing
        # differently would diverge on the first storage/provider change.
        rows = []
        for tup in document.get():
            tup.cast_to_schema(schema)
            rows.append(tup)
        return Table(rows)

    def _consume_pending_loop_state(self, executor: LoopEndOperator) -> None:
        # Run the matching consume that _process_state_frame deferred.
        #
        # The loop's input table is read from the Loop Start's input-port
        # materialization (see _read_loop_input_table for its lifetime). The
        # read is deferred to here rather than done when the state arrives: at
        # arrival time THIS worker's own materialization reader is still
        # streaming its input, and in runs where this read overlapped that
        # reader, the reader failed with S3 "Access Denied" (MinIO's answer
        # for a deleted key) while iterating a lazily-pinned snapshot of a doc
        # that region re-execution drops and recreates. Removing the overlap
        # made those failures stop; that the overlap CAUSED them is the
        # working hypothesis, not a ruled-out fact -- a doc dropped under a
        # live reader would break the reader with or without this read -- so
        # treat a recurrence as new evidence. By EndChannel the reader has
        # finished, so the two never overlap.
        if self._pending_loop_state is None:
            return
        pending = self._pending_loop_state
        self._pending_loop_state = None
        # Re-arm the duplicate guard here rather than relying on the scheduler
        # recreating workers each iteration: EndChannel is PORT_ALIGNMENT, so
        # nothing more can arrive on the port -- every duplicate is already in
        # by the time this consume runs.
        self._loop_state_consumed = False
        executor.attach_loop_table(self._read_loop_input_table())
        # A Loop End has exactly one input port (port 0); the generated
        # operator's process_state ignores the port anyway. That single input
        # port is also what makes THIS a safe place for the read above:
        # EndChannel is PORT_ALIGNMENT (dispatched once every channel of the
        # port has delivered it) and each reader emits its EndChannel only
        # after its iterator is exhausted, so with one input port every reader
        # of this worker has finished before the read. A second input port
        # would break that -- alignment is per port -- and silently
        # reintroduce the reader/read overlap this deferral removes.
        # The user's `update` runs here, on the main loop thread rather than
        # inside DataProcessor._executor_session, so capture its prints
        # explicitly -- otherwise they go to the worker's stdout instead of
        # the console.
        with replace_print(
            self.context.worker_id, self.context.console_message_manager.print_buf
        ):
            executor.process_state(pending, 0)

    def _jump_to_loop_start(
        self, executor: LoopEndOperator, coordinator_interface
    ) -> None:
        # Resolve the write address BEFORE the jump RPC so a misconfigured
        # loop does not rewind the schedule without a back-edge write.
        # Anything raised here (a missing URI, or a failed state write after
        # the jump) is reported by complete()'s guard as an operator-facing
        # error.
        uri = VFSURIFactory.state_uri(self._loop_start_base_uri())
        coordinator_interface.jump_to_operator_region(
            JumpToOperatorRegionRequest(OperatorIdentity(self._loop_start_id))
        )
        writer = DocumentFactory.create_document(uri, State.SCHEMA).writer("0")
        # The back-edge fires only after the matching LoopEnd consumed at
        # loop_counter == 0, so the next iteration's input starts at depth 0.
        writer.put_one(executor.state.to_tuple(0))
        writer.close()

    def _check_loop_state_arrived(self) -> None:
        # Keep the LOUD failure for a real loop state that lost its stamp
        # upstream. Forwarding unstamped states (above) is right for a body
        # operator's own boundary state, but it also swallows the symptom of
        # the bug class #6660/#6661 fixed: a hop that blanks the envelope makes
        # the loop's own state arrive unstamped, and forwarding it leaves
        # _loop_table None, so condition() returns False and the loop stops
        # after one iteration -- a WRONG RESULT reported as success.
        #
        # The two cases are told apart once the input port is done rather than
        # on arrival: a LoopEnd that forwarded an unstamped state and never
        # took a stamped one cannot have been looking at a body operator's
        # boundary state -- its own state never arrived. Deciding at EndChannel
        # is order-independent (the body operator's state may arrive before or
        # after the loop's; EndChannel is PORT_ALIGNMENT, so every channel on
        # the port has been drained) and reads nothing out of the State, so it
        # keeps working once the input table stops riding inside it (#6971).
        #
        # Called from _process_end_channel, NOT complete(): complete() runs
        # after port_completed has gone out for the input port and every output
        # port, and region completion is port-based, so a raise there would be
        # reported only once the coordinator already considers the region done.
        #
        # The check is deliberately narrow -- "forwarded an unstamped state AND
        # never took a stamped one", not "never took a stamped one". A LoopEnd
        # completing without any matching state is legal (see
        # LoopEndOperator.eval_condition's _loop_table guard); only positive
        # evidence that a boundary state arrived unstamped is a lost envelope.
        #
        # "Took a stamped one" is read off _loop_start_id, which only the
        # stamped branch writes and nothing clears -- NOT off the
        # _loop_state_consumed fan-in dedup flag, whose lifetime is owned by
        # the dedup and may end before this runs. Keying on the durable field
        # keeps this guard correct wherever it is called from.
        if self._forwarded_unstamped_state and not self._loop_start_id:
            raise RuntimeError(
                "Loop End received a loop-boundary state with no LoopStart "
                "stamp and never received its own (stamped) loop state: the "
                "loop envelope was lost upstream, so this loop would silently "
                "stop after one iteration"
            )

    def complete(self) -> None:
        """
        Complete the DataProcessor, marking state to COMPLETED, and notify the
        coordinator.
        """
        # flush the buffered console prints
        self._check_and_report_console_messages(force_flush=True)
        coordinator_interface = self._async_rpc_client.coordinator_stub()
        executor = self.context.executor_manager.executor
        if isinstance(executor, LoopEndOperator):
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
                # condition() is user code on the main loop thread, same as the
                # `update` in the deferred consume: capture its prints too.
                with replace_print(
                    self.context.worker_id,
                    self.context.console_message_manager.print_buf,
                ):
                    should_iterate = executor.condition()
                if should_iterate:
                    self._jump_to_loop_start(executor, coordinator_interface)
            except Exception as err:
                self.context.report_exception(err)
                self._check_exception()
                return
            # The opening flush above happened before condition() ran, and the
            # worker is about to shut down, so anything it printed would never
            # be sent. (_check_exception flushes on the error path.)
            self._check_and_report_console_messages(force_flush=True)
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
                # is setup config (InitializeExecutorRequest.loop_start_port_uris).
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

        # A LoopStart handles only the STAMPED case above. An UNstamped
        # counter-0 state at a LoopStart takes neither branch: it falls all the
        # way through to process_input_state at the bottom, and the operator's
        # process_state MERGES its keys into the loop variables. That is the
        # opposite of what the LoopEnd branch below does with the identical
        # frame, and the asymmetry is forced, not an oversight. The back-edge
        # writes the next iteration's variables to the LoopStart's own
        # input-port state URI with that very same "no loop" envelope
        # (_jump_to_loop_start -> State.to_tuple(0)), so an unstamped counter-0
        # frame at a LoopStart is indistinguishable from -- and normally IS --
        # the loop's own state. A LoopEnd may forward instead of consuming
        # because its inbound loop state is always stamped by the matching
        # LoopStart; a LoopStart has no such signal. Consequence of the merge:
        # an upstream or body operator emitting a key that collides with a loop
        # variable overwrites it, and one emitting `table` trips
        # _reserved_name_error in produce_state_on_finish (see #7248).

        if isinstance(executor, LoopEndOperator):
            if not frame.loop_start_id:
                # An UNstamped counter-0 state at a LoopEnd is not the loop's
                # own boundary state -- it was produced by a loop-body
                # operator's produce_state_on_start/finish (a public API on
                # both engine sides), which emits with the "no loop" envelope.
                # A real loop state is always stamped: the matching LoopStart
                # stamps its own id on every iteration's output state.
                # Forward it downstream unchanged, skipping the operator, like
                # any default pass-through: taking it would clobber the
                # captured back-jump id with "" and then fail the deferred
                # consume's bookkeeping-URI lookup for LoopStart ''.
                #
                # This MUST stay ahead of the dedup and the stash below: an
                # unstamped frame is not the loop's own state, so it may
                # neither mark that state as taken nor be handed to the
                # operator as the iteration's state.
                self._forwarded_unstamped_state = True
                self._emit_and_save_state(state, in_counter, frame.loop_start_id)
                self._check_and_process_control()
                return
            # Matching LoopEnd (in_counter == 0, stamped): it will consume this
            # state and jump back. Remember which LoopStart to jump to (it
            # rides the envelope) for complete()/_jump_to_loop_start, and STASH
            # the state -- the operator runs its update at EndChannel instead
            # of here, because the loop's input table is read from storage and
            # that read must not overlap this worker's own materialization
            # reader (see _consume_pending_loop_state). The LoopEnd emits no
            # state downstream on the matching consume, so deferring it
            # changes nothing observable outside the operator.
            #
            # With a branching loop body, each branch's reader replays the same
            # iteration's state, so this fires once per inbound link. The
            # deferred consume runs the user's update exactly once either way
            # (and run_update seeds its namespace from the incoming copy, so
            # re-running it on an identical copy would even be idempotent);
            # what this flag pins is WHICH copy is stashed -- the first,
            # instead of each later arrival silently overwriting
            # _pending_loop_state. The copies are the same state emitted by
            # the one matching LoopStart unless a body operator overrides
            # process_state to forward a modified one; rewriting the loop
            # state in a branching body is not supported (which copy arrives
            # first is scheduling-dependent either way), so take the first
            # deliberately.
            if self._loop_state_consumed:
                self._check_and_process_control()
                return
            self._loop_state_consumed = True
            self._loop_start_id = frame.loop_start_id
            self._pending_loop_state = state
            self._check_and_process_control()
            return

        self.context.state_processing_manager.current_input_state = state
        self.process_input_state(
            output_loop_counter=in_counter,
            output_loop_start_id=frame.loop_start_id,
        )
        self._check_and_process_control()

    def _process_start_channel(self) -> None:
        self._send_ecm_to_data_channels(
            "StartChannel", EmbeddedControlMessageType.NO_ALIGNMENT
        )
        self.process_input_state()

    def _process_end_channel(self) -> None:
        self.process_input_state()
        try:
            self._check_loop_state_arrived()
        except Exception as err:
            self.context.report_exception(err)
            self._check_exception()
        if self.context.exception_manager.has_exception():
            # A state-emission error was reported on the main loop thread (see
            # _emit_and_save_state). Hold the region BEFORE any loop work --
            # no storage read and no user `update` stacked on top of an
            # already-reported error -- and skip port_completed / complete()
            # so the coordinator does not mark the region complete (region
            # completion is port-based) with partial, single-iteration
            # results. The reported error surfaces instead of a false success.
            return
        # Run the deferred loop consume HERE, not in complete(): complete() is
        # the tail of this method, by which point port_completed has been sent
        # for every output port, so a failing read or `update` would be
        # reported after the coordinator already considers the region done
        # (region completion is port-based) -- a reported error that still
        # reads as success. This is still past the reader: EndChannel means it
        # finished streaming.
        executor = self.context.executor_manager.executor
        if isinstance(executor, LoopEndOperator):
            try:
                self._consume_pending_loop_state(executor)
            except Exception as err:
                self.context.report_exception(err)
                self._check_exception()
        if self.context.exception_manager.has_exception():
            # The deferred consume failed (the table read or the user's
            # update). Hold the region for the same reason as above.
            return
        self.process_input_tuple()

        input_port_id = self.context.input_manager.get_port_id(
            self.context.current_input_channel_id
        )

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
            # A PauseWorker command can land anywhere above -- the coordinator
            # pauses on its own schedule, and nothing between the last control
            # check and here drains control -- leaving the worker PAUSED.
            # complete() would then attempt PAUSED -> COMPLETED, which the
            # transition graph forbids (WORKER_STATE_TRANSITIONS, mirroring
            # Scala's WorkerStateManager), raising InvalidTransitionException
            # and killing this thread. Wait the pause out instead, matching
            # Scala's DPThread: while paused it only picks control channels, so
            # a paused worker never advances to completion.
            #
            # Guarded on is_paused() rather than draining control
            # unconditionally: _check_and_process_control blocks while the data
            # lane is disabled, and backpressure disables it too, so an
            # unguarded drain here could park a worker that is merely
            # backpressured. While paused the same call blocks until Resume
            # re-enables the lane and returns the worker to RUNNING.
            while self.context.pause_manager.is_paused():
                self._check_and_process_control()
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
