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
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.{PhysicalLink, PortIdentity}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.NO_ALIGNMENT
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ConsoleMessageTriggeredRequest,
  ControlInvocation,
  EmbeddedControlMessage,
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
import org.apache.texera.amber.engine.common.ambermessage.{
  DataFrame,
  StateFrame,
  WorkflowFIFOMessage
}
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer
import scala.util.control.ControlThrowable

/**
  * `startChannel` runs on a worker when the START_CHANNEL embedded control message reaches it. It
  * does three things, in this order:
  *
  *   1. resolves the input port from the channel the ECM arrived on
  *      (`inputManager.currentChannelId`), because `produceStateOnStart` is a per-port callback;
  *   2. forwards the marker downstream over the data channels, unaligned, so the whole downstream
  *      region sees the channel start;
  *   3. asks the operator for a boundary state and emits it if there is one.
  *
  * Step 2 precedes step 3 deliberately: a failing operator must not stop the marker from
  * propagating, or the downstream workers would never learn the channel started. Everything the
  * operator throws is absorbed by `ErrorUtils.safely` and reported through
  * `handleExecutorException` (console message + in-place pause); the RPC itself still succeeds, so
  * the coordinator never sees the failure as a control-message failure.
  *
  * These tests drive a real [[DataProcessor]] and read the worker's outgoing messages, so the ECM
  * and the emitted state are observed as the wire payloads a downstream worker would receive rather
  * than as mocked calls. `safely` (see `ErrorUtils.safely`) rethrows `scala.util.control.
  * ControlThrowable`, and also anything the handler it wraps is not defined at — here that handler
  * is `case e => dp.handleExecutorException(e)`, which matches every throwable, so in this call
  * site only `ControlThrowable` escapes and everything else, `Error` included, is swallowed. Both
  * directions are pinned below.
  */
class StartChannelHandlerSpec extends AnyFlatSpec {

  import StartChannelHandlerSpec._

  private val workerId = ActorVirtualIdentity("Worker:WF1-start-channel-main-0")
  private val upstreamWorkerId = ActorVirtualIdentity("Worker:WF1-upstream-main-0")
  private val otherUpstreamWorkerId = ActorVirtualIdentity("Worker:WF1-other-upstream-main-0")
  private val firstDownstreamWorkerId = ActorVirtualIdentity("Worker:WF1-downstream-main-0")
  private val secondDownstreamWorkerId = ActorVirtualIdentity("Worker:WF1-downstream-main-1")
  private val rpcContext = AsyncRPCContext(COORDINATOR, workerId)
  private val awaitTimeout = Duration.fromSeconds(5)

  /** The port of the channel the ECM arrives on. Non-zero, so a hard-coded 0 cannot pass. */
  private val currentPortId = PortIdentity(2)

  /** A second input port, wired to a channel that is *not* the current one. */
  private val idlePortId = PortIdentity(7)

  /** The output port both downstream links leave from. */
  private val outputPortId = PortIdentity(5)

  private val currentChannelId = ChannelIdentity(upstreamWorkerId, workerId, isControl = false)
  private val idleChannelId = ChannelIdentity(otherUpstreamWorkerId, workerId, isControl = false)
  private val firstDownstreamChannelId =
    ChannelIdentity(workerId, firstDownstreamWorkerId, isControl = false)
  private val secondDownstreamChannelId =
    ChannelIdentity(workerId, secondDownstreamWorkerId, isControl = false)
  private val coordinatorChannelId = ChannelIdentity(workerId, COORDINATOR, isControl = true)

  /** A data channel the fixture never registers, so its lazily created channel has no port. */
  private val unregisteredChannelId =
    ChannelIdentity(
      ActorVirtualIdentity("Worker:WF1-unregistered-main-0"),
      workerId,
      isControl = false
    )

  /**
    * Batch size of the fixture's output buffers. Larger than the number of tuples any test pushes,
    * so a tuple stays buffered until something flushes it on purpose.
    */
  private val batchSize = 10

  private val outputSchema: Schema = Schema().add("value", AttributeType.INTEGER)

  /**
    * What `sendECMToDataChannels(METHOD_START_CHANNEL, NO_ALIGNMENT)` puts on a data channel. The
    * command mapping is keyed by the receiver, so each data channel gets its own copy.
    */
  private def expectedStartChannelECM(
      receiver: ActorVirtualIdentity
  ): EmbeddedControlMessage =
    EmbeddedControlMessage(
      EmbeddedControlMessageIdentity("StartChannel"),
      NO_ALIGNMENT,
      Seq(),
      Map(
        receiver.name ->
          ControlInvocation(
            "StartChannel",
            EmptyRequest(),
            AsyncRPCContext(ActorVirtualIdentity(""), ActorVirtualIdentity("")),
            -1
          )
      )
    )

  /**
    * @param executor            the operator under the handler.
    * @param failOnEmitted       thrown by the output handler the first time a state leaves the
    *                            worker, which is how a failure *inside* `emitState` is simulated.
    * @param downstreamChannels  data channels to wire up; empty models a sink worker, which has no
    *                            downstream data channel at all.
    */
  private class Fixture(
      executor: OperatorExecutor,
      failOnEmitted: Option[Throwable] = None,
      downstreamChannels: Seq[(String, ChannelIdentity)] = Seq(
        "first" -> firstDownstreamChannelId,
        "second" -> secondDownstreamChannelId
      )
  ) {
    val sent: ArrayBuffer[WorkflowFIFOMessage] = ArrayBuffer()

    private val outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit = {
      case Right(msg) =>
        sent += msg
        failOnEmitted.foreach(failure =>
          if (msg.payload.isInstanceOf[StateFrame]) {
            throw failure
          }
        )
      case Left(_) => ()
    }

    val dp: DataProcessor =
      new DataProcessor(workerId, outputHandler, new LinkedBlockingQueue[DPInputQueueElement]())
    dp.executor = executor

    // Two input channels on two different ports; only one of them is the channel the ECM came in
    // on, so a handler that picked an arbitrary channel would read the wrong port.
    dp.inputGateway.getChannel(idleChannelId).setPortId(idlePortId)
    dp.inputGateway.getChannel(currentChannelId).setPortId(currentPortId)
    dp.inputManager.currentChannelId = currentChannelId

    // By default two downstream data channels, so "reaches every data channel" is distinguishable
    // from "reaches one of them", plus one control channel that must stay marker-free. Registering
    // a partitioning is also what creates the output buffer `emitState` writes to.
    dp.outputManager.addPort(outputPortId, outputSchema, None)
    downstreamChannels.foreach { case (name, channelId) => addDownstream(name, channelId) }
    dp.outputGateway.addOutputChannel(coordinatorChannelId)

    private def addDownstream(name: String, channelId: ChannelIdentity): Unit =
      dp.outputManager.addPartitionerWithPartitioning(
        PhysicalLink(
          PhysicalOpIdentity(OperatorIdentity("start-channel-spec-up"), "main"),
          outputPortId,
          PhysicalOpIdentity(OperatorIdentity(s"start-channel-spec-down-$name"), "main"),
          PortIdentity()
        ),
        OneToOnePartitioning(batchSize, Seq(channelId))
      )

    val handler: DataProcessorRPCHandlerInitializer = new DataProcessorRPCHandlerInitializer(dp)

    def startChannel(): EmptyReturn = await(handler.startChannel(EmptyRequest(), rpcContext))

    /** Buffers a tuple in every downstream buffer without flushing it (batch size is not reached). */
    def bufferOutputTuple(value: Int): Unit =
      dp.outputManager.passTupleToDownstream(TupleLike(value).enforceSchema(outputSchema), None)

    def markerChannels: Seq[ChannelIdentity] =
      sent.toSeq.collect {
        case WorkflowFIFOMessage(channelId, _, ecm: EmbeddedControlMessage)
            if ecm == expectedStartChannelECM(channelId.toWorkerId) =>
          channelId
      }

    /** Indices are asserted on, because the marker's position in the stream is part of the contract. */
    def indexOfFirstMarker: Int = sent.indexWhere(_.payload.isInstanceOf[EmbeddedControlMessage])

    def indexOfLastMarker: Int = sent.lastIndexWhere(_.payload.isInstanceOf[EmbeddedControlMessage])

    def emittedStates: Seq[StateFrame] =
      sent.toSeq.collect { case WorkflowFIFOMessage(_, _, frame: StateFrame) => frame }

    def stateChannels: Seq[ChannelIdentity] =
      sent.toSeq.collect { case WorkflowFIFOMessage(channelId, _, _: StateFrame) => channelId }

    def indexOfFirstState: Int = sent.indexWhere(_.payload.isInstanceOf[StateFrame])

    def dataFrames: Seq[(ChannelIdentity, DataFrame)] =
      sent.toSeq.collect {
        case WorkflowFIFOMessage(channelId, _, frame: DataFrame) =>
          (channelId, frame)
      }

    def indexOfLastDataFrame: Int = sent.lastIndexWhere(_.payload.isInstanceOf[DataFrame])

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
    * its pause set private, so the type is probed through `resume`: resuming some other type leaves
    * the worker paused (a global pause is still registered), and resuming `OperatorLogicPause`
    * clears it.
    */
  private def assertPausedByOperatorLogic(dp: DataProcessor): Unit = {
    assert(dp.pauseManager.isPaused)
    dp.pauseManager.resume(UserPause)
    assert(dp.pauseManager.isPaused)
    dp.pauseManager.resume(OperatorLogicPause)
    assert(!dp.pauseManager.isPaused)
  }

  behavior of "StartChannelHandler"

  it should "ask the operator for state on the port of the channel the ECM arrived on" in {
    val executor = new RecordingExecutor(_ => None)
    val fixture = new Fixture(executor)

    assert(fixture.startChannel() == EmptyReturn())

    // `currentPortId.id`, not the channel of the other input port and not the PortIdentity itself.
    assert(executor.startedPorts.toList == List(currentPortId.id))
  }

  it should "propagate an unaligned START_CHANNEL marker to every data channel" in {
    val fixture = new Fixture(new RecordingExecutor(_ => None))

    fixture.startChannel()

    val ecms = fixture.sent.toSeq.collect {
      case WorkflowFIFOMessage(channelId, _, ecm: EmbeddedControlMessage) => (channelId, ecm)
    }
    // Every data channel, not just the first one: a downstream worker that never sees the marker
    // never learns the channel started.
    assert(
      ecms.map(_._1).toSet == Set(firstDownstreamChannelId, secondDownstreamChannelId)
    )
    // Data channels only: the coordinator control channel is registered on the output gateway too,
    // and must not receive the marker.
    assert(ecms.size == 2)
    ecms.foreach {
      case (channelId, ecm) =>
        assert(ecm.ecmType == NO_ALIGNMENT)
        assert(ecm.id == EmbeddedControlMessageIdentity("StartChannel"))
        assert(ecm == expectedStartChannelECM(channelId.toWorkerId))
    }
  }

  it should "flush buffered output before the marker" in {
    val fixture = new Fixture(new RecordingExecutor(_ => None))
    fixture.bufferOutputTuple(1)
    // Nothing has left the worker yet: the tuple is sitting in the output buffers.
    assert(fixture.sent.isEmpty)

    fixture.startChannel()

    // Tuples produced before the marker belong before the marker in the channel's stream, so the
    // downstream worker sees them as part of the pre-marker data.
    assert(
      fixture.dataFrames.map(_._1).toSet == Set(firstDownstreamChannelId, secondDownstreamChannelId)
    )
    assert(fixture.dataFrames.forall(_._2.frame.map(_.getField[Int]("value")).toSeq == Seq(1)))
    assert(fixture.indexOfLastDataFrame < fixture.indexOfFirstMarker)
  }

  it should "emit the state the operator produced to every data channel, with no loop envelope" in {
    val state = State(Map("boundary" -> "start"))
    val fixture = new Fixture(new RecordingExecutor(_ => Some(state)))

    assert(fixture.startChannel() == EmptyReturn())

    // Exactly the state the operator returned, broadcast to every downstream channel. A state a
    // Scala handler originates carries the "no loop" defaults; the only Scala caller that passes an
    // envelope is the JVM hop in `DataProcessor.processState`, and it forwards the incoming one
    // rather than creating it.
    assert(fixture.emittedStates == Seq(StateFrame(state, 0L, ""), StateFrame(state, 0L, "")))
    assert(
      fixture.stateChannels.toSet == Set(firstDownstreamChannelId, secondDownstreamChannelId)
    )
  }

  it should "emit an empty state, because presence and not content gates the emission" in {
    // `isDefined` is the gate, so a State with no fields is still a state: empty is not absent.
    val emptyState = State(Map.empty[String, Any])
    val fixture = new Fixture(new RecordingExecutor(_ => Some(emptyState)))

    assert(fixture.startChannel() == EmptyReturn())

    assert(
      fixture.emittedStates == Seq(StateFrame(emptyState, 0L, ""), StateFrame(emptyState, 0L, ""))
    )
  }

  it should "emit no state when the operator produces none" in {
    val fixture = new Fixture(new RecordingExecutor(_ => None))

    assert(fixture.startChannel() == EmptyReturn())

    assert(fixture.emittedStates.isEmpty)
    // ...while the marker still went out.
    assert(fixture.markerChannels.size == 2)
  }

  it should "send the marker before the state it emits" in {
    val fixture = new Fixture(new RecordingExecutor(_ => Some(State(Map("k" -> 1)))))

    fixture.startChannel()

    assert(fixture.indexOfFirstMarker >= 0)
    assert(fixture.indexOfFirstState >= 0)
    // Every marker precedes every state, so no downstream channel receives state before the marker.
    assert(fixture.indexOfLastMarker < fixture.indexOfFirstState)
  }

  it should "emit nothing at all on a sink worker with no data channels" in {
    // A sink has no downstream data channel, so there is nowhere to put the marker or the state.
    // The operator is still asked for its boundary state; the state is simply dropped, because
    // `emitState` iterates the (empty) set of output buffers.
    val executor = new RecordingExecutor(_ => Some(State(Map("boundary" -> "start"))))
    val fixture = new Fixture(executor, downstreamChannels = Seq.empty)

    assert(fixture.startChannel() == EmptyReturn())

    assert(executor.startedPorts.toList == List(currentPortId.id))
    // Nothing left the worker: no marker, no state, and in particular nothing on the registered
    // control channel, which `sendECMToDataChannels` filters out.
    assert(fixture.sent.isEmpty)
  }

  it should "start the channel again on a second invocation" in {
    // The handler has no once-only guard, and it is reached from more than one place -- directly
    // from `StartHandler` for a source operator, and from the START_CHANNEL ECM an input-port
    // materialization reader thread emits when it starts -- so a second invocation repeats the
    // whole sequence rather than being ignored.
    val state = State(Map("boundary" -> "start"))
    val executor = new RecordingExecutor(_ => Some(state))
    val fixture = new Fixture(executor)

    assert(fixture.startChannel() == EmptyReturn())
    assert(fixture.startChannel() == EmptyReturn())

    assert(executor.startedPorts.toList == List(currentPortId.id, currentPortId.id))
    // One marker and one state per data channel, per invocation.
    assert(fixture.markerChannels.size == 4)
    assert(fixture.emittedStates.size == 4)
  }

  it should "swallow an operator exception, report it, and still reply successfully" in {
    val failure = new RuntimeException("produceStateOnStart blew up")
    val fixture = new Fixture(new RecordingExecutor(_ => throw failure))

    // The RPC succeeds: the failure is reported out-of-band, not as a control-message failure.
    assert(fixture.startChannel() == EmptyReturn())

    assert(fixture.emittedStates.isEmpty)
    // `handleExecutorException` sends a console message carrying the throwable...
    val consoleMessages = fixture.consoleMessages
    assert(consoleMessages.size == 1)
    assert(consoleMessages.head.consoleMessage.title == failure.toString)
    assert(consoleMessages.head.consoleMessage.workerId == workerId.name)
    // ...and pauses the worker in place.
    assertPausedByOperatorLogic(fixture.dp)
  }

  it should "swallow a failure raised while emitting the state" in {
    // The emission itself is inside the try, not just the operator call: a state that cannot be
    // put on the wire is reported the same way a failing `produceStateOnStart` is.
    val failure = new RuntimeException("emitting the state blew up")
    val fixture =
      new Fixture(new RecordingExecutor(_ => Some(State(Map("k" -> 1)))), Some(failure))

    assert(fixture.startChannel() == EmptyReturn())

    // One downstream channel got the state and then the emission failed -- which one is not fixed,
    // since `emitState` iterates a HashMap of buffers. The failure is routed to
    // `handleExecutorException` instead of escaping the handler.
    assert(fixture.emittedStates.size == 1)
    assert(fixture.consoleMessages.map(_.consoleMessage.title) == Seq(failure.toString))
    assertPausedByOperatorLogic(fixture.dp)
  }

  it should "swallow an operator Error as well" in {
    // `ErrorUtils.safely` guards only against ControlThrowable, so an Error is handled like any
    // other throwable rather than escaping.
    val failure = new Error("produceStateOnStart failed hard")
    val fixture = new Fixture(new RecordingExecutor(_ => throw failure))

    assert(fixture.startChannel() == EmptyReturn())

    assert(fixture.consoleMessages.map(_.consoleMessage.title) == Seq(failure.toString))
    assertPausedByOperatorLogic(fixture.dp)
  }

  it should "still have propagated the marker when state production throws" in {
    val fixture = new Fixture(new RecordingExecutor(_ => throw new RuntimeException("boom")))

    fixture.startChannel()

    // The marker goes out before the try block, so every downstream channel still learns the
    // channel started.
    assert(fixture.indexOfFirstMarker == 0)
    assert(
      fixture.markerChannels.toSet == Set(firstDownstreamChannelId, secondDownstreamChannelId)
    )
  }

  it should "fail outright when the current channel has no port assigned" in {
    // `NetworkInputGateway.getChannel` lazily creates a channel whose portId is None, so an
    // unregistered channel does not fail fast — it fails at `getPortId`, with an
    // IllegalStateException. That call sits *before* the try block, so unlike every operator
    // failure it is NOT routed to `handleExecutorException`: it escapes as an RPC failure, and the
    // marker is never sent. This is characterization of that asymmetry, not an endorsement of it.
    val fixture = new Fixture(new RecordingExecutor(_ => None))
    fixture.dp.inputManager.currentChannelId = unregisteredChannelId

    val failure = intercept[IllegalStateException] {
      fixture.startChannel()
    }
    assert(failure.getMessage.startsWith("portId has not been set for channel"))

    // The exact mirror image of "the marker already went out when the executor throws": here
    // nothing went out at all.
    assert(fixture.markerChannels.isEmpty)
    assert(fixture.sent.isEmpty)
    assert(fixture.consoleMessages.isEmpty)
    assert(!fixture.dp.pauseManager.isPaused)
  }

  it should "let a ControlThrowable escape after the marker was sent" in {
    // The one throwable `safely` refuses to handle: it is rethrown, so it propagates out of the
    // handler instead of being reported and paused on.
    val fixture = new Fixture(new RecordingExecutor(_ => throw new ControlThrowable {}))

    intercept[ControlThrowable] {
      fixture.startChannel()
    }

    assert(fixture.indexOfFirstMarker == 0)
    assert(
      fixture.markerChannels.toSet == Set(firstDownstreamChannelId, secondDownstreamChannelId)
    )
    assert(fixture.consoleMessages.isEmpty)
    assert(!fixture.dp.pauseManager.isPaused)
  }
}

object StartChannelHandlerSpec {

  /** Records the port `produceStateOnStart` was called with and returns whatever the test wants. */
  class RecordingExecutor(onStart: Int => Option[State]) extends OperatorExecutor {
    val startedPorts: ArrayBuffer[Int] = ArrayBuffer()

    override def produceStateOnStart(port: Int): Option[State] = {
      startedPorts += port
      onStart(port)
    }

    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
  }
}
