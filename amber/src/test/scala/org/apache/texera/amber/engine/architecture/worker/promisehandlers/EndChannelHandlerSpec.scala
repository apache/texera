/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.{Await, Duration, Future}
import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{
  AttributeType,
  FinalizeExecutor,
  FinalizePort,
  Schema,
  Tuple,
  TupleLike
}
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.{PhysicalLink, PortIdentity}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ConsoleMessageTriggeredRequest,
  ControlInvocation,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  MainThreadDelegateMessage
}
import org.apache.texera.amber.engine.architecture.worker.{
  DataProcessor,
  DataProcessorRPCHandlerInitializer,
  OperatorLogicPause,
  UserPause
}
import org.apache.texera.amber.engine.common.ambermessage.{StateFrame, WorkflowFIFOMessage}
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer
import scala.util.control.ControlThrowable

/**
  * `endChannel` runs on a worker when the END_CHANNEL embedded control message reaches it, i.e.
  * when one input channel has delivered everything it will ever deliver. It is the mirror of
  * [[StartChannelHandlerSpec]]'s handler and does four things:
  *
  *   1. resolves the input port from the channel the ECM arrived on and marks that port completed;
  *   2. resets the channel's input batch (`initBatch(channelId, Array.empty)`), so the DP loop's
  *      `hasUnfinishedInput` stops reporting leftover tuples on a channel that has just ended;
  *   3. asks the operator for a boundary state (`produceStateOnFinish`) and emits it if there is
  *      one, then hands the operator's finish output (`onFinishMultiPort`) to the output iterator,
  *      in that order;
  *   4. appends the input port's `FinalizePort` marker to the output stream;
  *   5. finalizes the *output* ports, but only once every input port is completed.
  *
  * Steps 1, 2, 4 and 5 sit outside the `try`, so an operator that throws in step 3 — at either of
  * its two call sites — still gets its port marked completed and its markers appended. What the
  * operator throws is absorbed by `ErrorUtils.safely` and reported through
  * `handleExecutorException` (console message + in-place pause), leaving the RPC itself
  * successful; the one exception is `scala.util.control.ControlThrowable`, which `safely`
  * rethrows. Both directions are pinned below.
  *
  * The `forall` in step 5 is why the fixture always registers two input ports: a worker whose
  * other input port is still open must NOT finalize its output, or the downstream region would
  * see the executor finish while data is still arriving on the second port. The fixture pins that
  * `forall`, but only over link-fed ports: `InputManager.isPortCompleted` branches, and for a port
  * fed from materialization it reports the reader THREAD's `finished` flag and ignores
  * `completed` entirely. That arm needs storage-backed input ports, which this fixture cannot
  * build without real Iceberg documents, so substituting `getPort(p).completed` for
  * `isPortCompleted(p)` is not distinguishable here — a known, stated gap rather than coverage.
  *
  * `produceStateOnFinish` has no override in main today — `OperatorExecutor` returns `None` for
  * every port — so the "operator produced a state" arm is reachable only through a test executor.
  * It is a declared extension point of the same shape as `produceStateOnStart`, and the emission
  * it drives (no loop envelope) is what a Loop End downstream depends on, so it is pinned here
  * rather than left unspecified.
  *
  * These tests drive a real [[DataProcessor]] with no ActorSystem: the emitted state is read off
  * the worker's outgoing wire messages, and the port markers are read off the DP output iterator
  * the DP thread would drain.
  */
class EndChannelHandlerSpec extends AnyFlatSpec {

  import EndChannelHandlerSpec._

  private val workerId = ActorVirtualIdentity("Worker:WF1-end-channel-main-0")
  private val upstreamWorkerId = ActorVirtualIdentity("Worker:WF1-upstream-main-0")
  private val otherUpstreamWorkerId = ActorVirtualIdentity("Worker:WF1-other-upstream-main-0")
  private val downstreamWorkerId = ActorVirtualIdentity("Worker:WF1-downstream-main-0")
  private val rpcContext = AsyncRPCContext(COORDINATOR, workerId)
  private val awaitTimeout = Duration.fromSeconds(5)

  /** The port of the channel the ECM arrives on. Non-zero, so a hard-coded 0 cannot pass. */
  private val currentPortId = PortIdentity(2)

  /** A second input port, wired to a channel that is *not* the one the ECM arrives on. */
  private val otherPortId = PortIdentity(7)

  /** The single output port. Distinct from both input port ids. */
  private val outputPortId = PortIdentity(5)

  private val currentChannelId = ChannelIdentity(upstreamWorkerId, workerId, isControl = false)
  private val otherChannelId = ChannelIdentity(otherUpstreamWorkerId, workerId, isControl = false)
  private val downstreamChannelId =
    ChannelIdentity(workerId, downstreamWorkerId, isControl = false)
  private val coordinatorChannelId = ChannelIdentity(workerId, COORDINATOR, isControl = true)

  /** Larger than the number of tuples any test produces, so nothing flushes by accident. */
  private val batchSize = 10

  private val schema: Schema = Schema().add("value", AttributeType.INTEGER)

  /**
    * @param executor                  the operator under the handler.
    * @param otherInputPortCompleted   whether the *other* input port has already finished. The
    *                                  common single-input case behaves like `true`: the channel
    *                                  being ended is the last one open.
    */
  private class Fixture(
      executor: OperatorExecutor,
      otherInputPortCompleted: Boolean = true
  ) {
    val sent: ArrayBuffer[WorkflowFIFOMessage] = ArrayBuffer()

    private val outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit = {
      case Right(msg) => sent += msg
      case Left(_)    => ()
    }

    val dp: DataProcessor =
      new DataProcessor(workerId, outputHandler, new LinkedBlockingQueue[DPInputQueueElement]())
    dp.executor = executor

    // Two input ports on two different channels; only one of them is the channel the ECM came in
    // on, so a handler that picked an arbitrary channel would read the wrong port.
    dp.inputManager.addPort(currentPortId, schema, List.empty, List.empty)
    dp.inputManager.addPort(otherPortId, schema, List.empty, List.empty)
    dp.inputGateway.getChannel(currentChannelId).setPortId(currentPortId)
    dp.inputGateway.getChannel(otherChannelId).setPortId(otherPortId)
    // Seed an unconsumed input batch on the channel about to be ended. `endChannel` resets it
    // (`initBatch(channelId, Array.empty)`), and without a seed that reset is invisible: the
    // batch would already be null and `hasUnfinishedInput` already false.
    dp.inputManager.initBatch(
      currentChannelId,
      Array(TupleLike(9).enforceSchema(schema))
    )
    dp.inputManager.getPort(otherPortId).completed = otherInputPortCompleted

    // One downstream data channel, which is also what creates the output buffer `emitState`
    // writes to, plus a control channel that carries the console message on the failure paths.
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.outputManager.addPartitionerWithPartitioning(
      PhysicalLink(
        PhysicalOpIdentity(OperatorIdentity("end-channel-spec-up"), "main"),
        outputPortId,
        PhysicalOpIdentity(OperatorIdentity("end-channel-spec-down"), "main"),
        PortIdentity()
      ),
      OneToOnePartitioning(batchSize, Seq(downstreamChannelId))
    )
    dp.outputGateway.addOutputChannel(coordinatorChannelId)

    val handler: DataProcessorRPCHandlerInitializer = new DataProcessorRPCHandlerInitializer(dp)

    def endChannel(): EmptyReturn = await(handler.endChannel(EmptyRequest(), rpcContext))

    /** Everything the DP thread would pull out of the output iterator, in order. */
    def drainOutput(): List[TupleLike] =
      dp.outputManager.outputIterator.map(_._1).toList

    def emittedStates: Seq[StateFrame] =
      sent.toSeq.collect { case WorkflowFIFOMessage(_, _, frame: StateFrame) => frame }

    def consoleMessages: Seq[ConsoleMessageTriggeredRequest] =
      sent.toSeq
        .collect {
          case WorkflowFIFOMessage(_, _, invocation: ControlInvocation) =>
            invocation.command
        }
        .collect { case request: ConsoleMessageTriggeredRequest => request }
  }

  private def await[T](future: Future[T]): T = Await.result(future, awaitTimeout)

  /**
    * `handleExecutorException` pauses with `OperatorLogicPause` specifically. `PauseManager` keeps
    * its pause set private, so the type is probed through `resume`: resuming some other type
    * leaves the worker paused, and resuming `OperatorLogicPause` clears it.
    */
  private def assertPausedByOperatorLogic(dp: DataProcessor): Unit = {
    assert(dp.pauseManager.isPaused)
    dp.pauseManager.resume(UserPause)
    assert(dp.pauseManager.isPaused)
    dp.pauseManager.resume(OperatorLogicPause)
    assert(!dp.pauseManager.isPaused)
  }

  behavior of "EndChannelHandler"

  it should "complete the input port of the channel the ECM arrived on, and only that one" in {
    val executor = new RecordingExecutor()
    val fixture = new Fixture(executor, otherInputPortCompleted = false)

    assert(fixture.endChannel() == EmptyReturn())

    assert(fixture.dp.inputManager.isPortCompleted(currentPortId))
    assert(!fixture.dp.inputManager.isPortCompleted(otherPortId))
    // `currentPortId.id`, not the other input port's and not the PortIdentity itself — and for
    // BOTH finish callbacks, which take the port independently.
    assert(executor.statePorts.toList == List(currentPortId.id))
    assert(executor.finishOutputPorts.toList == List(currentPortId.id))
  }

  it should "emit the boundary state the operator produced on finish, with no loop envelope" in {
    val state = State(Map("boundary" -> "end"))
    val fixture = new Fixture(new RecordingExecutor(onFinishState = _ => Some(state)))

    assert(fixture.endChannel() == EmptyReturn())

    // Exactly the state the operator returned. A state a Scala handler originates carries the
    // "no loop" defaults (loopCounter 0, loopStartId ""); only the JVM hop in
    // `DataProcessor.processState` forwards an incoming envelope, and it never creates one.
    assert(fixture.emittedStates == Seq(StateFrame(state, 0L, "")))
  }

  it should "emit an empty state, because presence and not content gates the emission" in {
    // `isDefined` is the gate, so a State with no fields is still a state: empty is not absent.
    val emptyState = State(Map.empty[String, Any])
    val fixture = new Fixture(new RecordingExecutor(onFinishState = _ => Some(emptyState)))

    assert(fixture.endChannel() == EmptyReturn())

    assert(fixture.emittedStates == Seq(StateFrame(emptyState, 0L, "")))
  }

  it should "emit no state when the operator produces none, but still finalize the input port" in {
    val fixture = new Fixture(new RecordingExecutor())

    // The channel still has a seeded, unconsumed tuple when the ECM arrives...
    assert(fixture.dp.inputManager.hasUnfinishedInput)

    assert(fixture.endChannel() == EmptyReturn())

    // ...and does not after: the channel has delivered everything it ever will, so `endChannel`
    // resets the input batch. `hasUnfinishedInput` is what the DP loop consults before pulling
    // another tuple, so leaving it set would keep feeding a channel that has just been declared
    // ended and whose port was marked completed one statement earlier.
    assert(!fixture.dp.inputManager.hasUnfinishedInput)

    assert(fixture.emittedStates.isEmpty)
    // "No state" is the ordinary case, not a failure: reaching into the empty Option would be
    // absorbed by `safely` and surface as a console message and a pause rather than as a crash,
    // so the absence of both is what proves the guard is doing the work.
    assert(fixture.consoleMessages.isEmpty)
    assert(!fixture.dp.pauseManager.isPaused)
    assert(fixture.drainOutput().contains(FinalizePort(currentPortId, input = true)))
  }

  it should "put the operator's finish output ahead of the input port marker" in {
    val fixture = new Fixture(
      new RecordingExecutor(
        onFinishTuples = _ =>
          Iterator(
            TupleLike(1).enforceSchema(schema),
            TupleLike(2).enforceSchema(schema)
          )
      )
    )

    fixture.endChannel()

    val output = fixture.drainOutput()
    // What is pinned here is that the operator's finish output reaches the output iterator at
    // all, and in the order the operator produced it.
    assert(output.collect { case tuple: Tuple => tuple.getField[Int]("value") } == List(1, 2))
    // The marker follows those tuples, but that is `DPOutputIterator`'s doing and not this
    // handler's: `next()` drains `outputIter` to exhaustion before it touches `queue`
    // (OutputManager.DPOutputIterator), so no statement ordering inside `endChannel` could put an
    // appended marker ahead of the finish tuples. Read this as a characterization of the drain
    // order, not as a pin on where `appendSpecialTupleToEnd` is called; that placement is pinned
    // by the two exception tests below, which assert the marker survives a throwing operator.
    assert(output.indexOf(FinalizePort(currentPortId, input = true)) == 2)
  }

  it should "swallow an operator exception raised at finish, report it, and still reply successfully" in {
    val failure = new RuntimeException("onFinish blew up")
    val state = State(Map("boundary" -> "end"))
    // An operator that produces a boundary state AND then fails while flushing its finish output.
    // The state is asked for first, so it is emitted before the failure rather than lost with it.
    val fixture = new Fixture(
      new RecordingExecutor(onFinishState = _ => Some(state), onFinishTuples = _ => throw failure)
    )

    // The RPC succeeds: the failure is reported out-of-band, not as a control-message failure.
    assert(fixture.endChannel() == EmptyReturn())

    // The boundary state made it out. This is what pins the ORDER of the two finish callbacks:
    // ask for the state, then take the finish output. Taking the output first would mean an
    // operator that throws there never has `produceStateOnFinish` called at all, and the state a
    // downstream Loop End consumes would be silently dropped instead of emitted.
    assert(fixture.emittedStates == Seq(StateFrame(state, 0L, "")))

    // `handleExecutorException` sends a console message carrying the throwable...
    val consoleMessages = fixture.consoleMessages
    assert(consoleMessages.size == 1)
    assert(consoleMessages.head.consoleMessage.title == failure.toString)
    assert(consoleMessages.head.consoleMessage.workerId == workerId.name)
    // ...and pauses the worker in place.
    assertPausedByOperatorLogic(fixture.dp)
    // Steps 4 AND 5 sit outside the try, so a failed operator still gets its input-port marker
    // appended and — this fixture's other input port having already completed — its output ports
    // finalized. A handler that skipped finalization on the failure path would leave the
    // downstream region waiting on a worker that will never produce anything again, so the whole
    // drained stream is asserted here rather than just the presence of the input marker.
    assert(
      fixture.drainOutput() == List(
        FinalizePort(currentPortId, input = true),
        FinalizePort(outputPortId, input = false),
        FinalizeExecutor()
      )
    )
  }

  it should "swallow an operator Error as well" in {
    // `ErrorUtils.safely` guards only against ControlThrowable, so an Error is handled like any
    // other throwable rather than escaping.
    val failure = new Error("produceStateOnFinish failed hard")
    val fixture = new Fixture(new RecordingExecutor(onFinishState = _ => throw failure))

    assert(fixture.endChannel() == EmptyReturn())

    assert(fixture.consoleMessages.map(_.consoleMessage.title) == Seq(failure.toString))
    assertPausedByOperatorLogic(fixture.dp)
    // The marker is appended outside the try for the FIRST throw site too, not only the second:
    // this operator dies in `produceStateOnFinish`, before anything else in the block runs, and
    // the downstream still learns the input port is finalized.
    assert(fixture.drainOutput().contains(FinalizePort(currentPortId, input = true)))
    // Step 2 is outside the try as well, and this is the throw site that proves it: the batch
    // reset happens before the operator is ever consulted, so a channel that has just ended
    // cannot keep feeding the DP loop even when the operator dies at its very first callback.
    assert(!fixture.dp.inputManager.hasUnfinishedInput)
  }

  it should "let a ControlThrowable escape the finish handler" in {
    // The one throwable `safely` refuses to handle: it is rethrown, so it propagates out of the
    // handler instead of being reported and paused on.
    val fixture =
      new Fixture(new RecordingExecutor(onFinishState = _ => throw new ControlThrowable {}))

    intercept[ControlThrowable] {
      fixture.endChannel()
    }

    assert(fixture.consoleMessages.isEmpty)
    assert(!fixture.dp.pauseManager.isPaused)
    // Characterization of the asymmetry, not an endorsement: the port was already marked
    // completed before the try block, yet nothing downstream is told, because the markers are
    // appended after it.
    assert(fixture.dp.inputManager.isPortCompleted(currentPortId))
    assert(fixture.drainOutput().isEmpty)
  }

  it should "leave the output unfinalized while another input port is still open" in {
    val fixture = new Fixture(new RecordingExecutor(), otherInputPortCompleted = false)

    fixture.endChannel()

    // Only the input port's own marker. Finalizing the output here would tell the downstream
    // region the executor is done while the second input port is still delivering.
    assert(fixture.drainOutput() == List(FinalizePort(currentPortId, input = true)))
  }

  it should "finalize the output once the last input port completes" in {
    val fixture = new Fixture(new RecordingExecutor(), otherInputPortCompleted = true)

    fixture.endChannel()

    assert(
      fixture.drainOutput() == List(
        FinalizePort(currentPortId, input = true),
        FinalizePort(outputPortId, input = false),
        FinalizeExecutor()
      )
    )
  }
}

object EndChannelHandlerSpec {

  /**
    * Records the port each finish callback was called with, and produces whatever the test wants.
    *
    * The two callbacks record separately on purpose. `onFinishMultiPort` is a thin forward to
    * `onFinish(port)` (see `OperatorExecutor`), and a multi-input operator — HashJoinProbe,
    * Difference, Aggregate all override `onFinish(port)` — flushes the buffer of exactly the port
    * it is handed, so the operand of each call is observable behaviour rather than bookkeeping.
    * Recording only one of them would leave the other call site's port unpinned.
    */
  class RecordingExecutor(
      onFinishState: Int => Option[State] = _ => None,
      onFinishTuples: Int => Iterator[TupleLike] = _ => Iterator.empty
  ) extends OperatorExecutor {

    /** Ports handed to `produceStateOnFinish`. */
    val statePorts: ArrayBuffer[Int] = ArrayBuffer()

    /** Ports handed to `onFinish`, i.e. what `onFinishMultiPort` forwarded. */
    val finishOutputPorts: ArrayBuffer[Int] = ArrayBuffer()

    override def produceStateOnFinish(port: Int): Option[State] = {
      statePorts += port
      onFinishState(port)
    }

    override def onFinish(port: Int): Iterator[TupleLike] = {
      finishOutputPorts += port
      onFinishTuples(port)
    }

    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
  }
}
