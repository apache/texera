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

package org.apache.texera.amber.engine.architecture.worker

import com.fasterxml.jackson.annotation.{JsonProperty, JsonValue}
import org.apache.texera.amber.core.executor.{ExecFactory, OpExecWithClassName, OperatorExecutor}
import org.apache.texera.amber.core.state.{State, StateReferencing}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity._
import org.apache.texera.amber.core.workflow.{PhysicalLink, PortIdentity}
import org.apache.texera.amber.core.workflow.WorkflowContext.DEFAULT_WORKFLOW_ID
import org.apache.texera.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import org.apache.texera.amber.engine.architecture.logreplay.{ReplayLogManager, ReplayLogRecord}
import org.apache.texera.amber.engine.architecture.messaginglayer.WorkerTimerService
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ConsoleMessageTriggeredRequest,
  ConsoleMessageType,
  ControlInvocation => ControlInvocationMessage,
  EmbeddedControlMessage,
  EmbeddedControlMessageType,
  EmptyRequest,
  InitializeExecutorRequest
}
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.{
  METHOD_END_CHANNEL,
  METHOD_FLUSH_NETWORK_BUFFER,
  METHOD_INITIALIZE_EXECUTOR,
  METHOD_OPEN_EXECUTOR,
  METHOD_START_CHANNEL
}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  MainThreadDelegateMessage
}
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerState.READY
import org.apache.texera.amber.engine.common.ambermessage.{
  DataFrame,
  StateFrame,
  WorkflowFIFOMessage
}
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.storage.SequentialRecordStorage
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.operator.ifStatement.{IfOpDesc, IfOpExec}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.VirtualIdentityUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer

class DataProcessorSpec extends AnyFlatSpec with MockFactory with Matchers with BeforeAndAfterEach {

  import DataProcessorSpec._

  private val testOpId = PhysicalOpIdentity(OperatorIdentity("testop"), "main")
  private val upstreamOpId = PhysicalOpIdentity(OperatorIdentity("sender"), "main")
  private val testWorkerId: ActorVirtualIdentity = VirtualIdentityUtils.createWorkerIdentity(
    DEFAULT_WORKFLOW_ID,
    testOpId,
    0
  )
  private val senderWorkerId: ActorVirtualIdentity = VirtualIdentityUtils.createWorkerIdentity(
    DEFAULT_WORKFLOW_ID,
    upstreamOpId,
    0
  )

  private val executor = mock[OperatorExecutor]
  private val inputPortId = PortIdentity()
  private val outputPortId = PortIdentity()
  private val outputHandler = mock[Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit]
  private val adaptiveBatchingMonitor = mock[WorkerTimerService]
  private val schema: Schema = Schema().add("field1", AttributeType.INTEGER)
  private val tuples: Array[Tuple] = (0 until 400)
    .map(i => TupleLike(i).enforceSchema(schema))
    .toArray
  private val logStorage = SequentialRecordStorage.getStorage[ReplayLogRecord](None)
  private val logManager: ReplayLogManager =
    ReplayLogManager.createLogManager(logStorage, "none", x => {})
  private val endChannelPayload = EmbeddedControlMessage(
    EmbeddedControlMessageIdentity("EndChannel"),
    EmbeddedControlMessageType.PORT_ALIGNMENT,
    Seq(),
    Map(
      testWorkerId.name ->
        ControlInvocation(
          METHOD_END_CHANNEL.getBareMethodName,
          EmptyRequest(),
          AsyncRPCContext(ActorVirtualIdentity(""), ActorVirtualIdentity("")),
          -1
        )
    )
  )

  def mkDataProcessor: DataProcessor = {
    val dp: DataProcessor = new DataProcessor(
      testWorkerId,
      outputHandler,
      inputMessageQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    )
    dp.initTimerService(adaptiveBatchingMonitor)
    dp
  }

  "data processor" should "process data messages" in {
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    (outputHandler.apply _).expects(*).once()
    (executor.open _).expects().once()
    tuples.foreach { x =>
      (
          (
              tuple: Tuple,
              input: Int
          ) => executor.processTupleMultiPort(tuple, input)
      )
        .expects(x, 0)
    }
    (
        (
          input: Int
        ) => executor.produceStateOnFinish(input)
    )
      .expects(0)
      .returning(None)
    (
        (
          input: Int
        ) => executor.onFinishMultiPort(input)
    )
      .expects(
        0
      )
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    (adaptiveBatchingMonitor.stopAdaptiveBatching _).expects().once()
    (() => executor.getWarnings).expects().returning(Seq.empty)
    (executor.close _).expects().once()
    (outputHandler.apply _).expects(*).anyNumberOfTimes()
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.processDCM(
      ChannelIdentity(COORDINATOR, testWorkerId, isControl = true),
      ControlInvocation(
        METHOD_OPEN_EXECUTOR,
        EmptyRequest(),
        AsyncRPCContext(COORDINATOR, testWorkerId),
        0
      )
    )
    dp.processDataPayload(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      DataFrame(tuples)
    )
    while (dp.inputManager.hasUnfinishedInput || dp.outputManager.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
    dp.processECM(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      endChannelPayload,
      logManager
    )

    while (dp.inputManager.hasUnfinishedInput || dp.outputManager.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
  }

  "data processor" should "process control messages during data processing" in {
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    (outputHandler.apply _).expects(*).anyNumberOfTimes()
    (executor.open _).expects().once()
    tuples.foreach { x =>
      (
          (
              tuple: Tuple,
              input: Int
          ) => executor.processTupleMultiPort(tuple, input)
      )
        .expects(x, 0)
    }
    (
        (
          input: Int
        ) => executor.produceStateOnFinish(input)
    )
      .expects(0)
      .returning(None)
    (
        (
          input: Int
        ) => executor.onFinishMultiPort(input)
    )
      .expects(0)
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.processDCM(
      ChannelIdentity(COORDINATOR, testWorkerId, isControl = true),
      ControlInvocation(
        METHOD_OPEN_EXECUTOR,
        EmptyRequest(),
        AsyncRPCContext(COORDINATOR, testWorkerId),
        0
      )
    )
    dp.processDataPayload(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      DataFrame(tuples)
    )
    while (dp.inputManager.hasUnfinishedInput || dp.outputManager.hasUnfinishedOutput) {
      dp.processDCM(
        ChannelIdentity(COORDINATOR, testWorkerId, isControl = true),
        ControlInvocation(
          METHOD_FLUSH_NETWORK_BUFFER,
          EmptyRequest(),
          AsyncRPCContext(COORDINATOR, testWorkerId),
          1
        )
      )
      dp.continueDataProcessing()
    }
    (adaptiveBatchingMonitor.stopAdaptiveBatching _).expects().once()
    (() => executor.getWarnings).expects().returning(Seq.empty)
    (executor.close _).expects().once()
    dp.processECM(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      endChannelPayload,
      logManager
    )
    while (dp.inputManager.hasUnfinishedInput || dp.outputManager.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
  }

  "data processor" should "process a state frame and emit the produced state" in {
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    val emitted = scala.collection.mutable.ArrayBuffer[WorkflowFIFOMessage]()
    (outputHandler.apply _)
      .expects(*)
      .onCall { (m: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        m.foreach(emitted += _); ()
      }
      .anyNumberOfTimes()
    val inputState = State(Map("field1" -> 1))
    (
        (
            state: State,
            port: Int
        ) => executor.processState(state, port)
    )
      .expects(inputState, 0)
      .returning(Some(inputState))
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    // A downstream partitioner makes emitState observable: the produced state is sent
    // downstream as a StateFrame, captured via the output handler.
    dp.outputManager.addPartitionerWithPartitioning(
      PhysicalLink(testOpId, outputPortId, upstreamOpId, inputPortId),
      OneToOnePartitioning(1, Seq(ChannelIdentity(testWorkerId, senderWorkerId, isControl = false)))
    )
    dp.processDataPayload(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      StateFrame(inputState)
    )
    assert(emitted.exists(_.payload.isInstanceOf[StateFrame]))
  }

  "data processor" should "carry the loop envelope through a state pass-through unchanged" in {
    // Loop operators are Python-only, so a JVM operator inside a loop body
    // only ever FORWARDS the StateFrame loop envelope (loop_counter /
    // loop_start_id); the +1/-1 bookkeeping lives in the Python runtime.
    // A dropped envelope zeroes the counter and blanks the id, which breaks
    // the matching LoopEnd's back-jump ("no loop-back state URI configured
    // for LoopStart ''") -- so pin exact envelope equality on the emitted
    // frame, not just that a StateFrame came out.
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    val emitted = scala.collection.mutable.ArrayBuffer[WorkflowFIFOMessage]()
    (outputHandler.apply _)
      .expects(*)
      .onCall { (m: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        m.foreach(emitted += _); ()
      }
      .anyNumberOfTimes()
    val inputState = State(Map("i" -> 1))
    (
        (
            state: State,
            port: Int
        ) => executor.processState(state, port)
    )
      .expects(inputState, 0)
      .returning(Some(inputState))
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.outputManager.addPartitionerWithPartitioning(
      PhysicalLink(testOpId, outputPortId, upstreamOpId, inputPortId),
      OneToOnePartitioning(1, Seq(ChannelIdentity(testWorkerId, senderWorkerId, isControl = false)))
    )
    dp.processDataPayload(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      StateFrame(inputState, loopCounter = 2L, loopStartId = "outer-loop")
    )
    val stateFrames = emitted.map(_.payload).collect { case sf: StateFrame => sf }
    assert(
      stateFrames.toList == List(StateFrame(inputState, 2L, "outer-loop")),
      s"envelope must ride through unchanged, got: $stateFrames"
    )
  }

  "data processor" should "process a copy of a state message once, whichever input port it arrives on" in {
    // Two body branches meet at this operator: message A (K = 2) arrives once per branch, and one
    // branch has meanwhile sent a distinct message B (K = 3). Only the first copy of A is
    // registered, handed to the operator and sent on; B is processed, and the copy of A arriving
    // after it is not even registered, so the operator's state keeps B's value.
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    val emitted = scala.collection.mutable.ArrayBuffer[WorkflowFIFOMessage]()
    (outputHandler.apply _)
      .expects(*)
      .onCall { (m: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        m.foreach(emitted += _); ()
      }
      .anyNumberOfTimes()
    val inputState = State(Map("K" -> 2))
    val changedState = State(Map("K" -> 3))
    ((state: State, port: Int) => executor.processState(state, port))
      .expects(inputState, 0)
      .returning(Some(inputState))
      .once()
    ((state: State, port: Int) => executor.processState(state, port))
      .expects(changedState, 0)
      .returning(Some(changedState))
      .once()
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    val secondPort = PortIdentity(1)
    dp.inputManager.addPort(secondPort, schema, List.empty, List.empty)
    val direct = ChannelIdentity(senderWorkerId, testWorkerId, isControl = false)
    val filtered =
      ChannelIdentity(ActorVirtualIdentity("filtered"), testWorkerId, isControl = false)
    dp.inputGateway.getChannel(direct).setPortId(inputPortId)
    dp.inputGateway.getChannel(filtered).setPortId(secondPort)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.outputManager.addPartitionerWithPartitioning(
      PhysicalLink(testOpId, outputPortId, upstreamOpId, inputPortId),
      OneToOnePartitioning(1, Seq(ChannelIdentity(testWorkerId, senderWorkerId, isControl = false)))
    )
    val original = StateFrame(inputState, 0L, "loop", "A")
    dp.processDataPayload(direct, original)
    dp.processDataPayload(filtered, original)
    dp.processDataPayload(direct, StateFrame(changedState, 0L, "loop", "B"))
    dp.processDataPayload(filtered, original)
    assert(!dp.pauseManager.isPaused)
    assert(executor.state.contains(changedState))
    // An unchanged state is sent on as the message it is, keeping its id.
    val forwarded = emitted.map(_.payload).collect { case frame: StateFrame => frame }
    assert(forwarded.map(_.messageId).toList == List("A", "B"))
  }

  "data processor" should "send on the state its operator handed back as the same message without writing it out as JSON" in {
    // The default processState hands back the very state it got, which is the same message: it
    // keeps its id without both states being serialized to compare them. An equal state the
    // operator built anew is compared by its JSON, and is the same message too.
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    val emitted = scala.collection.mutable.ArrayBuffer[WorkflowFIFOMessage]()
    (outputHandler.apply _)
      .expects(*)
      .onCall { (m: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        m.foreach(emitted += _); ()
      }
      .anyNumberOfTimes()
    val value = new JsonWriteCounter
    val handedBack = State(Map("v" -> value))
    val rebuilt = State(Map("n" -> 1))
    ((state: State, port: Int) => executor.processState(state, port))
      .expects(handedBack, 0)
      .returning(Some(handedBack))
    ((state: State, port: Int) => executor.processState(state, port))
      .expects(rebuilt, 0)
      .returning(Some(State(Map("n" -> 1L))))
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway.getChannel(senderChannel).setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.outputManager.addPartitionerWithPartitioning(
      PhysicalLink(testOpId, outputPortId, upstreamOpId, inputPortId),
      OneToOnePartitioning(1, Seq(ChannelIdentity(testWorkerId, senderWorkerId, isControl = false)))
    )
    dp.processDataPayload(senderChannel, StateFrame(handedBack, 0L, "loop", "A"))
    dp.processDataPayload(senderChannel, StateFrame(rebuilt, 0L, "loop", "B"))

    assert(value.writes == 0)
    val forwarded = emitted.map(_.payload).collect { case frame: StateFrame => frame }
    assert(forwarded.map(_.messageId).toList == List("A", "B"))
    assert(!dp.pauseManager.isPaused)
  }

  "data processor" should "not emit when processState yields None" in {
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    val emitted = scala.collection.mutable.ArrayBuffer[WorkflowFIFOMessage]()
    (outputHandler.apply _)
      .expects(*)
      .onCall { (m: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        m.foreach(emitted += _); ()
      }
      .anyNumberOfTimes()
    val inputState = State(Map("field1" -> 2))
    (
        (
            state: State,
            port: Int
        ) => executor.processState(state, port)
    )
      .expects(inputState, 0)
      .returning(None)
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    // Same downstream partitioner as the emit test; because processState returns None,
    // no StateFrame must be emitted.
    dp.outputManager.addPartitionerWithPartitioning(
      PhysicalLink(testOpId, outputPortId, upstreamOpId, inputPortId),
      OneToOnePartitioning(1, Seq(ChannelIdentity(testWorkerId, senderWorkerId, isControl = false)))
    )
    dp.processDataPayload(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      StateFrame(inputState)
    )
    assert(!emitted.exists(_.payload.isInstanceOf[StateFrame]))
  }

  "data processor" should "send a loop's message on unchanged when processState yields None for it" in {
    // Body operators forward their loop's messages: a processState that returns None for a state
    // stamped with its Loop Start still has that message sent on unchanged, under its id and at
    // the depth it arrived at. A state without the stamp is not sent on.
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    val emitted = ArrayBuffer[WorkflowFIFOMessage]()
    (outputHandler.apply _)
      .expects(*)
      .onCall { (m: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        m.foreach(emitted += _); ()
      }
      .anyNumberOfTimes()
    val loopMessage = State(Map("i" -> 0L))
    val ownState = State(Map("score" -> 1L))
    ((state: State, port: Int) => executor.processState(state, port))
      .expects(loopMessage, 0)
      .returning(None)
    ((state: State, port: Int) => executor.processState(state, port))
      .expects(ownState, 0)
      .returning(None)
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway.getChannel(senderChannel).setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.outputManager.addPartitionerWithPartitioning(
      PhysicalLink(testOpId, outputPortId, upstreamOpId, inputPortId),
      OneToOnePartitioning(1, Seq(ChannelIdentity(testWorkerId, senderWorkerId, isControl = false)))
    )
    dp.processDataPayload(senderChannel, StateFrame(loopMessage, 1L, "loop", "loop-start-A"))
    dp.processDataPayload(senderChannel, StateFrame(ownState, 0L, "", "P"))

    val forwarded = emitted.map(_.payload).collect { case frame: StateFrame => frame }.toList
    assert(forwarded == List(StateFrame(loopMessage, 1L, "loop", "loop-start-A")))
    assert(!dp.pauseManager.isPaused)
  }

  "data processor" should "send nothing on for a loop's message whose processState throws" in {
    // A processState that throws hands nothing back: the error is reported, the worker pauses, and
    // the loop's message it got is not sent on.
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    val emitted = ArrayBuffer[WorkflowFIFOMessage]()
    (outputHandler.apply _)
      .expects(*)
      .onCall { (m: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        m.foreach(emitted += _); ()
      }
      .anyNumberOfTimes()
    val loopMessage = State(Map("i" -> 0L))
    val error = new RuntimeException("boom on a loop's message")
    ((state: State, port: Int) => executor.processState(state, port))
      .expects(loopMessage, 0)
      .throwing(error)
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway.getChannel(senderChannel).setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.outputManager.addPartitionerWithPartitioning(
      PhysicalLink(testOpId, outputPortId, upstreamOpId, inputPortId),
      OneToOnePartitioning(1, Seq(ChannelIdentity(testWorkerId, senderWorkerId, isControl = false)))
    )
    dp.processDataPayload(senderChannel, StateFrame(loopMessage, 1L, "loop", "loop-start-A"))
    dp.outputManager.flush()

    assert(statesSent(emitted).isEmpty)
    assert(consoleTitles(emitted) == List(error.toString))
    dp.pauseManager.isPaused shouldBe true
  }

  "data processor" should "handle an exception thrown while processing a state frame" in {
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    (outputHandler.apply _).expects(*).anyNumberOfTimes()
    val inputState = State(Map("field1" -> 3))
    (
        (
            state: State,
            port: Int
        ) => executor.processState(state, port)
    )
      .expects(inputState, 0)
      .throwing(new RuntimeException("boom on state"))
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    noException should be thrownBy {
      dp.processDataPayload(
        ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
        StateFrame(inputState)
      )
    }
    // handleExecutorException must engage an operator-logic pause.
    dp.pauseManager.isPaused shouldBe true
  }

  "data processor" should "handle an exception thrown while processing an input tuple" in {
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    (outputHandler.apply _).expects(*).anyNumberOfTimes()
    (
        (
            tuple: Tuple,
            input: Int
        ) => executor.processTupleMultiPort(tuple, input)
    )
      .expects(tuples.head, 0)
      .throwing(new RuntimeException("boom on tuple"))
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    noException should be thrownBy {
      dp.processDataPayload(
        ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
        DataFrame(Array(tuples.head))
      )
    }
    // handleExecutorException must engage an operator-logic pause.
    dp.pauseManager.isPaused shouldBe true
  }

  // ---------------------------------------------------------------------------
  // An operator whose setting refers to a loop variable
  // ---------------------------------------------------------------------------

  private val senderChannel = ChannelIdentity(senderWorkerId, testWorkerId, isControl = false)
  private val coordinatorChannel = ChannelIdentity(COORDINATOR, testWorkerId, isControl = true)

  private val startChannelPayload = EmbeddedControlMessage(
    EmbeddedControlMessageIdentity("StartChannel"),
    EmbeddedControlMessageType.NO_ALIGNMENT,
    Seq(),
    Map(
      testWorkerId.name ->
        ControlInvocation(
          METHOD_START_CHANNEL.getBareMethodName,
          EmptyRequest(),
          AsyncRPCContext(ActorVirtualIdentity(""), ActorVirtualIdentity("")),
          -1
        )
    )
  )

  /**
    * A data processor whose executor is initialized the way the coordinator initializes a worker,
    * from `descString` for `LoopLimitExec`, with everything the worker sends collected.
    */
  private def initializedWith(
      descString: String
  ): (DataProcessor, LoopLimitExec, ArrayBuffer[WorkflowFIFOMessage]) = {
    val dp = mkDataProcessor
    val sent = ArrayBuffer[WorkflowFIFOMessage]()
    (outputHandler.apply _)
      .expects(*)
      .onCall { (m: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        m.foreach(sent += _); ()
      }
      .anyNumberOfTimes()
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    dp.stateManager.transitTo(READY)
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway.getChannel(senderChannel).setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.outputManager.addPartitionerWithPartitioning(
      PhysicalLink(testOpId, outputPortId, upstreamOpId, inputPortId),
      OneToOnePartitioning(1, Seq(ChannelIdentity(testWorkerId, senderWorkerId, isControl = false)))
    )
    dp.processDCM(
      coordinatorChannel,
      ControlInvocation(
        METHOD_INITIALIZE_EXECUTOR,
        InitializeExecutorRequest(
          totalWorkerCount = 1,
          opExecInitInfo = OpExecWithClassName(classOf[LoopLimitExec].getName, descString),
          isSource = false,
          loopStartPortUris = Map.empty
        ),
        AsyncRPCContext(COORDINATOR, testWorkerId),
        0
      )
    )
    (dp, dp.executor.asInstanceOf[LoopLimitExec], sent)
  }

  private def openExecutor(dp: DataProcessor): Unit =
    dp.processDCM(
      coordinatorChannel,
      ControlInvocation(
        METHOD_OPEN_EXECUTOR,
        EmptyRequest(),
        AsyncRPCContext(COORDINATOR, testWorkerId),
        1
      )
    )

  private def drain(dp: DataProcessor): Unit =
    while (dp.inputManager.hasUnfinishedInput || dp.outputManager.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }

  private def statesSent(sent: ArrayBuffer[WorkflowFIFOMessage]): List[State] =
    sent.map(_.payload).collect { case frame: StateFrame => frame.frame }.toList

  private def tuplesSent(sent: ArrayBuffer[WorkflowFIFOMessage]): List[Tuple] =
    sent.map(_.payload).collect { case DataFrame(frame) => frame.toList }.flatten.toList

  private def consoleTitles(sent: ArrayBuffer[WorkflowFIFOMessage]): List[String] =
    sent
      .map(_.payload)
      .collect {
        case invocation: org.apache.texera.amber.engine.architecture.rpc.controlcommands.ControlInvocation =>
          invocation.command
      }
      .collect { case request: ConsoleMessageTriggeredRequest => request.consoleMessage.title }
      .toList

  "data processor" should "run an operator whose setting refers to a loop variable as any other, the variable written in as the state arrives" in {
    val (dp, exec, sent) = initializedWith(limitReferringToN)
    // The operator's own executor, built at worker start from the descString with its placeholder.
    assert(exec.setting.limit == 0)

    openExecutor(dp)
    dp.processECM(senderChannel, startChannelPayload, logManager)
    val first = State(Map("n" -> 2L))
    dp.processDataPayload(senderChannel, StateFrame(first, loopCounter = 1))
    dp.processDataPayload(senderChannel, DataFrame(tuples.take(5)))
    drain(dp)
    val later = State(Map("n" -> 9L))
    dp.processDataPayload(senderChannel, StateFrame(later))

    // open() and produceStateOnStart see the placeholder; each processState sees its message
    // registered and the variable written; a state after the first tuple no longer writes, even
    // one from a more deeply nested loop.
    assert(
      exec.calls.toList == List(
        ("open", 0, None),
        ("start", 0, None),
        ("state", 2, Some(first)),
        ("state", 2, Some(later))
      )
    )
    // What the worker sends on is what the operator returned for each callback.
    assert(
      statesSent(sent) == List(
        State(Map("start" -> 0)),
        State(Map("seen" -> 2L, "limit" -> 2)),
        State(Map("seen" -> 9L, "limit" -> 2))
      )
    )
    assert(tuplesSent(sent) == tuples.take(2).toList)
    assert(consoleTitles(sent).isEmpty)
    assert(!dp.pauseManager.isPaused)
  }

  "data processor" should "write a later message's loop variable before the first tuple, and keep the bound value after it" in {
    val (dp, exec, sent) = initializedWith(limitReferringToN)
    openExecutor(dp)
    // Two distinct messages from the same loop before the first tuple, not copies of one: the later
    // value is written. An enclosing loop's message after them is not.
    dp.processDataPayload(senderChannel, StateFrame(State(Map("n" -> 2L)), 0L, "", "A"))
    dp.processDataPayload(senderChannel, StateFrame(State(Map("n" -> 3L)), 0L, "", "B"))
    dp.processDataPayload(senderChannel, StateFrame(State(Map("n" -> 1L)), 1L, "outer", "C"))
    dp.processDataPayload(senderChannel, DataFrame(tuples.take(5)))
    drain(dp)
    // After the first tuple, a later message from the same loop neither fails nor changes the bound
    // limit.
    dp.processDataPayload(senderChannel, StateFrame(State(Map("n" -> 4L)), 0L, "", "D"))
    dp.processDataPayload(senderChannel, DataFrame(tuples.slice(5, 10)))
    drain(dp)

    assert(
      exec.calls.map { case (call, limit, _) => (call, limit) }.toList ==
        List(("open", 0), ("state", 2), ("state", 3), ("state", 3), ("state", 3))
    )
    assert(tuplesSent(sent) == tuples.take(3).toList)
    assert(consoleTitles(sent).isEmpty)
    assert(!dp.pauseManager.isPaused)
  }

  "data processor" should "send a state its operator changed as a new message, after the loop's message it came from" in {
    val (dp, _, sent) = initializedWith(limitReferringToN)
    openExecutor(dp)
    val loopMessage = State(Map("n" -> 2L))
    dp.processDataPayload(senderChannel, StateFrame(loopMessage, 1L, "outer-loop", "A"))
    dp.processDataPayload(senderChannel, StateFrame(State(Map("n" -> 3L)), 0L, "", "P"))

    // LoopLimitExec returns a state of its own, so what it sends on is a new message: it keeps the
    // loop envelope but gets an id of its own. The change does not replace a loop's message, one
    // stamped with its Loop Start: that goes on unchanged first, under its id, so a copy of it
    // arriving after the change where the loop body meets again downstream is one the receiver
    // already processed. A change to a state without that stamp replaces it.
    val frames = sent.map(_.payload).collect { case frame: StateFrame => frame }.toList
    assert(
      frames.map(frame => (frame.frame, frame.loopCounter, frame.loopStartId)) == List(
        (loopMessage, 1L, "outer-loop"),
        (State(Map("seen" -> 2L, "limit" -> 2)), 1L, "outer-loop"),
        (State(Map("seen" -> 3L, "limit" -> 3)), 0L, "")
      )
    )
    assert(frames.head.messageId == "A")
    val newIds = frames.tail.map(_.messageId)
    assert(newIds.forall(id => id.nonEmpty && id != "A" && id != "P"))
    assert(newIds.distinct.size == 2)
  }

  "data processor" should "send on a state its operator handed back with its keys in another order as the same message" in {
    // The order of a map's keys says nothing about its values, also in a nested map: what the
    // operator hands back is the message that came in, under its id, and no change follows it.
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    val emitted = ArrayBuffer[WorkflowFIFOMessage]()
    (outputHandler.apply _)
      .expects(*)
      .onCall { (m: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        m.foreach(emitted += _); ()
      }
      .anyNumberOfTimes()
    val incoming = State(Map("a" -> 1L, "b" -> Map("c" -> 1L, "d" -> 2L)))
    ((state: State, port: Int) => executor.processState(state, port))
      .expects(incoming, 0)
      .returning(Some(State(Map("b" -> Map("d" -> 2L, "c" -> 1L), "a" -> 1L))))
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway.getChannel(senderChannel).setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.outputManager.addPartitionerWithPartitioning(
      PhysicalLink(testOpId, outputPortId, upstreamOpId, inputPortId),
      OneToOnePartitioning(1, Seq(ChannelIdentity(testWorkerId, senderWorkerId, isControl = false)))
    )
    dp.processDataPayload(senderChannel, StateFrame(incoming, 0L, "loop", "A"))

    val forwarded = emitted.map(_.payload).collect { case frame: StateFrame => frame }
    assert(forwarded.map(_.messageId).toList == List("A"))
    assert(!dp.pauseManager.isPaused)
  }

  "data processor" should "send a state whose 2 its operator turned into 2.0 as a new message" in {
    // Scala's == calls 2L and 2.0 equal, while their JSON tells them apart: only the very state the
    // operator got back is taken as unchanged without comparing JSON.
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    val emitted = ArrayBuffer[WorkflowFIFOMessage]()
    (outputHandler.apply _)
      .expects(*)
      .onCall { (m: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        m.foreach(emitted += _); ()
      }
      .anyNumberOfTimes()
    val incoming = State(Map("n" -> 2L))
    ((state: State, port: Int) => executor.processState(state, port))
      .expects(incoming, 0)
      .returning(Some(State(Map("n" -> 2.0))))
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway.getChannel(senderChannel).setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.outputManager.addPartitionerWithPartitioning(
      PhysicalLink(testOpId, outputPortId, upstreamOpId, inputPortId),
      OneToOnePartitioning(1, Seq(ChannelIdentity(testWorkerId, senderWorkerId, isControl = false)))
    )
    dp.processDataPayload(senderChannel, StateFrame(incoming, 0L, "", "A"))

    val forwarded = emitted.map(_.payload).collect { case frame: StateFrame => frame }.toList
    assert(forwarded.map(_.frame) == List(State(Map("n" -> 2.0))))
    assert(forwarded.map(_.frame.values("n").getClass) == List(classOf[java.lang.Double]))
    assert(forwarded.head.messageId.nonEmpty && forwarded.head.messageId != "A")
    assert(!dp.pauseManager.isPaused)
  }

  // The built-in If, run as the engine runs it: its data port (1) depends on its Condition port
  // (0), so the engine runs the Condition port first, before the If has any output port, and then
  // its data port. Each input port has a sender of its own, and each output port, False (0) and
  // True (1), a receiver of its own.

  private val conditionChannel =
    ChannelIdentity(ActorVirtualIdentity("condition-sender"), testWorkerId, isControl = false)
  private val dataChannel =
    ChannelIdentity(ActorVirtualIdentity("data-sender"), testWorkerId, isControl = false)
  private val falseReceiver = ActorVirtualIdentity("false-receiver")
  private val trueReceiver = ActorVirtualIdentity("true-receiver")

  /** A worker running the built-in If on `even`, with both input ports and no output port yet. */
  private def ifWorker(sent: ArrayBuffer[WorkflowFIFOMessage]): DataProcessor = {
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    val dp = new DataProcessor(
      testWorkerId,
      (message: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        message.foreach(sent += _),
      inputMessageQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    )
    dp.initTimerService(adaptiveBatchingMonitor)
    dp.stateManager.transitTo(READY)
    val ifDesc = new IfOpDesc()
    ifDesc.conditionName = "even"
    dp.executor = new IfOpExec(objectMapper.writeValueAsString(ifDesc))
    for (
      (port, channel) <- List(PortIdentity() -> conditionChannel, PortIdentity(1) -> dataChannel)
    ) {
      dp.inputManager.addPort(port, schema, List.empty, List.empty)
      dp.inputGateway.getChannel(channel).setPortId(port)
    }
    dp
  }

  /** Gives the If its output ports, as the engine does once the If has read its Condition port. */
  private def addIfOutputPorts(dp: DataProcessor): Unit =
    for (
      (port, receiver) <- List(PortIdentity() -> falseReceiver, PortIdentity(1) -> trueReceiver)
    ) {
      dp.outputManager.addPort(port, schema, None)
      dp.outputManager.addPartitionerWithPartitioning(
        PhysicalLink(testOpId, port, upstreamOpId, inputPortId),
        OneToOnePartitioning(1, Seq(ChannelIdentity(testWorkerId, receiver, isControl = false)))
      )
    }

  /** The tuples a worker sent, each with the receiver it went to. */
  private def tuplesTo(
      sent: ArrayBuffer[WorkflowFIFOMessage]
  ): List[(ActorVirtualIdentity, Tuple)] =
    sent.toList.flatMap {
      case WorkflowFIFOMessage(channel, _, DataFrame(frame)) => frame.map(channel.toWorkerId -> _)
      case _                                                 => Nil
    }

  /** The state frames a worker sent, each with the receiver it went to. */
  private def statesTo(
      sent: ArrayBuffer[WorkflowFIFOMessage]
  ): List[(ActorVirtualIdentity, StateFrame)] =
    sent.toList.collect {
      case WorkflowFIFOMessage(channel, _, frame: StateFrame) => channel.toWorkerId -> frame
    }

  "data processor" should "route an If by the condition a change adds after the loop's message it came from" in {
    // Inside a loop, an operator whose processState adds the If's condition to the loop's message
    // sends that message on first, without the condition, under its id, and the change after it.
    // The If's Condition port gets the two messages before the If has any output port, and its data
    // port gets the same two messages again and then the tuples. A worker running the built-in If
    // keeps its route on the first message, takes the route the change sets, and sends the tuples
    // to the True port. An If that failed on the first message would report an error and pause the
    // worker.
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    val addingSent = ArrayBuffer[WorkflowFIFOMessage]()
    val adding = new DataProcessor(
      testWorkerId,
      (message: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        message.foreach(addingSent += _),
      inputMessageQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    )
    adding.initTimerService(adaptiveBatchingMonitor)
    adding.stateManager.transitTo(READY)
    adding.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    adding.inputGateway.getChannel(senderChannel).setPortId(inputPortId)
    adding.executor = executor
    val loopMessage = State(Map("i" -> 0L))
    ((state: State, port: Int) => executor.processState(state, port))
      .expects(loopMessage, 0)
      .returning(Some(State(Map("i" -> 0L, "even" -> true))))
    adding.outputManager.addPort(outputPortId, schema, None)
    adding.outputManager.addPartitionerWithPartitioning(
      PhysicalLink(testOpId, outputPortId, upstreamOpId, inputPortId),
      OneToOnePartitioning(1, Seq(ChannelIdentity(testWorkerId, senderWorkerId, isControl = false)))
    )
    adding.processDataPayload(senderChannel, StateFrame(loopMessage, 0L, "loop", "loop-start-A"))
    val toIf = addingSent.map(_.payload).collect { case frame: StateFrame => frame }.toList
    assert(toIf.map(_.frame.values.contains("even")) == List(false, true))
    assert(toIf.head.messageId == "loop-start-A")

    val ifSent = ArrayBuffer[WorkflowFIFOMessage]()
    val branching = ifWorker(ifSent)
    toIf.foreach(branching.processDataPayload(conditionChannel, _))
    assert(ifSent.isEmpty)
    addIfOutputPorts(branching)
    toIf.foreach(branching.processDataPayload(dataChannel, _))
    branching.processDataPayload(dataChannel, DataFrame(tuples.take(2)))
    drain(branching)
    branching.outputManager.flush()

    assert(consoleTitles(ifSent).isEmpty)
    assert(!branching.pauseManager.isPaused)
    // The tuples go to the True port's receiver only.
    assert(tuplesTo(ifSent) == tuples.take(2).toList.map(trueReceiver -> _))
    // What the If sent before it had output ports reached no one. It processes the copies on its
    // data port again and sends both messages on, under their ids and in the order they came, to
    // each port's receiver.
    for (receiver <- List(falseReceiver, trueReceiver)) {
      assert(statesTo(ifSent).filter(_._1 == receiver).map(_._2) == toIf)
    }
  }

  "data processor" should "send the loop's message on from an If that reads its Condition port before it has output ports" in {
    // Loop Start's message reaches both ports of the If, and its condition sends the tuples to the
    // False port. On the Condition port the If takes the route without recording the message's id;
    // once it has its output ports, it processes the copy on the data port again and sends it on
    // under its id to each port's receiver, routes the tuples by the condition and drops a further
    // copy.
    val ifSent = ArrayBuffer[WorkflowFIFOMessage]()
    val dp = ifWorker(ifSent)
    val loopMessage =
      StateFrame(State(Map("i" -> 1L, "even" -> false)), 0L, "loop", "loop-start-A")
    dp.processDataPayload(conditionChannel, loopMessage)
    assert(ifSent.isEmpty)
    addIfOutputPorts(dp)
    dp.processDataPayload(dataChannel, loopMessage)
    dp.processDataPayload(dataChannel, DataFrame(tuples.take(2)))
    drain(dp)
    dp.processDataPayload(dataChannel, loopMessage)
    dp.outputManager.flush()

    assert(consoleTitles(ifSent).isEmpty)
    assert(!dp.pauseManager.isPaused)
    assert(
      statesTo(ifSent).sortBy(_._1.name) == List(falseReceiver, trueReceiver).map(_ -> loopMessage)
    )
    assert(tuplesTo(ifSent) == tuples.take(2).toList.map(falseReceiver -> _))
  }

  "data processor" should "route an If by a change that reached only its Condition port, before the If had output ports" in {
    // An operator on the Condition branch adds the condition: it sends Loop Start's message on,
    // without the condition, and then its change, both only to the If's Condition port. The data
    // port gets Loop Start's message, then the tuples. The If takes its route from the change it
    // processed before it had output ports, keeps it on the copy of Loop Start's message on its
    // data port, which it sends on to each port's receiver, and sends the tuples to the False port.
    val ifSent = ArrayBuffer[WorkflowFIFOMessage]()
    val dp = ifWorker(ifSent)
    val loopMessage = StateFrame(State(Map("i" -> 1L)), 0L, "loop", "loop-start-A")
    val change = StateFrame(State(Map("i" -> 1L, "even" -> false)), 0L, "loop", "B")
    dp.processDataPayload(conditionChannel, loopMessage)
    dp.processDataPayload(conditionChannel, change)
    assert(ifSent.isEmpty)
    addIfOutputPorts(dp)
    dp.processDataPayload(dataChannel, loopMessage)
    dp.processDataPayload(dataChannel, DataFrame(tuples.take(2)))
    drain(dp)
    dp.outputManager.flush()

    assert(consoleTitles(ifSent).isEmpty)
    assert(!dp.pauseManager.isPaused)
    assert(tuplesTo(ifSent) == tuples.take(2).toList.map(falseReceiver -> _))
    assert(
      statesTo(ifSent).sortBy(_._1.name) == List(falseReceiver, trueReceiver).map(_ -> loopMessage)
    )
  }

  "data processor" should "route an If outside a loop by a state that reached only its Condition port" in {
    // Outside a loop, the state an operator sends the If's Condition port has an id and no Loop
    // Start stamp. The If takes its route from it before it has output ports and sends the tuples
    // on its data port to the False port.
    val ifSent = ArrayBuffer[WorkflowFIFOMessage]()
    val dp = ifWorker(ifSent)
    dp.processDataPayload(conditionChannel, StateFrame(State(Map("even" -> false)), 0L, "", "P"))
    assert(ifSent.isEmpty)
    addIfOutputPorts(dp)
    dp.processDataPayload(dataChannel, DataFrame(tuples.take(3)))
    drain(dp)
    dp.outputManager.flush()

    assert(consoleTitles(ifSent).isEmpty)
    assert(!dp.pauseManager.isPaused)
    assert(tuplesTo(ifSent) == tuples.take(3).toList.map(falseReceiver -> _))
  }

  "data processor" should "fail the first tuple of an operator whose setting refers to a loop variable no state message carried" in {
    val (dp, exec, sent) = initializedWith(limitReferringToN)
    openExecutor(dp)

    dp.processDataPayload(senderChannel, DataFrame(tuples.take(1)))

    // The operator never sees the tuple: it would be processed with the placeholder.
    assert(exec.tuplesSeen == 0)
    assert(
      consoleTitles(sent) == List(
        new IllegalStateException(
          "property /limit refers to loop variable n, but no state message carried it"
        ).toString
      )
    )
    dp.pauseManager.isPaused shouldBe true
  }

  "data processor" should "bind the references of an executor that replaces one already bound" in {
    // Reconfiguration and checkpoint restore install a new executor: its references are its own.
    val (dp, exec, sent) = initializedWith(limitReferringToN)
    openExecutor(dp)
    dp.processDataPayload(senderChannel, StateFrame(State(Map("n" -> 2L))))
    dp.processDataPayload(senderChannel, DataFrame(tuples.take(1)))
    drain(dp)
    assert(exec.tuplesSeen == 1)

    val replacement = loopLimitExec()
    dp.executor = replacement
    dp.processDataPayload(senderChannel, DataFrame(tuples.slice(1, 2)))

    assert(replacement.tuplesSeen == 0)
    assert(
      consoleTitles(sent) == List(
        new IllegalStateException(
          "property /limit refers to loop variable n, but no state message carried it"
        ).toString
      )
    )
  }

  "data processor" should "handle an exception thrown while advancing the output iterator" in {
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    (outputHandler.apply _).expects(*).anyNumberOfTimes()
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    // Poison the output iterator: hasNext is true so continueDataProcessing routes
    // into outputOneTuple, but next() throws to exercise the catch branch.
    dp.outputManager.outputIterator.setTupleOutput(
      new Iterator[(TupleLike, Option[PortIdentity])] {
        override def hasNext: Boolean = true
        override def next(): (TupleLike, Option[PortIdentity]) =
          throw new RuntimeException("boom on next")
      }
    )
    assert(dp.outputManager.hasUnfinishedOutput)
    noException should be thrownBy {
      dp.continueDataProcessing()
    }
    // handleExecutorException must pause the operator and reset the output iterator to empty.
    dp.pauseManager.isPaused shouldBe true
    dp.outputManager.hasUnfinishedOutput shouldBe false
  }

  "data processor" should "emit executor warnings as PRINT console messages at finalize without pausing" in {
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    val emitted = scala.collection.mutable.ArrayBuffer[WorkflowFIFOMessage]()
    (outputHandler.apply _)
      .expects(*)
      .onCall { (m: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]) =>
        m.foreach(emitted += _); ()
      }
      .anyNumberOfTimes()
    (executor.open _).expects().once()
    (
        (
            tuple: Tuple,
            input: Int
        ) => executor.processTupleMultiPort(tuple, input)
    )
      .expects(tuples.head, 0)
    (
        (
          input: Int
        ) => executor.produceStateOnFinish(input)
    )
      .expects(0)
      .returning(None)
    (
        (
          input: Int
        ) => executor.onFinishMultiPort(input)
    )
      .expects(0)
    val warningText = "WARNING: skipped row 3 - value 'oops' cannot be read as INTEGER"
    (() => executor.getWarnings).expects().returning(Seq(warningText))
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    (adaptiveBatchingMonitor.stopAdaptiveBatching _).expects().once()
    (executor.close _).expects().once()
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.processDCM(
      ChannelIdentity(COORDINATOR, testWorkerId, isControl = true),
      ControlInvocation(
        METHOD_OPEN_EXECUTOR,
        EmptyRequest(),
        AsyncRPCContext(COORDINATOR, testWorkerId),
        0
      )
    )
    dp.processDataPayload(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      DataFrame(Array(tuples.head))
    )
    while (dp.inputManager.hasUnfinishedInput || dp.outputManager.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
    dp.processECM(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      endChannelPayload,
      logManager
    )
    while (dp.inputManager.hasUnfinishedInput || dp.outputManager.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }

    // The warning must reach the coordinator as a PRINT console message carrying the
    // executor's warning line as its title, and it must not pause the run.
    val consoleMessages = emitted.flatMap(_.payload match {
      case ControlInvocationMessage(_, req: ConsoleMessageTriggeredRequest, _, _) =>
        Some(req.consoleMessage)
      case _ => None
    })
    assert(
      consoleMessages.exists(m => m.msgType == ConsoleMessageType.PRINT && m.title == warningText)
    )
    dp.pauseManager.isPaused shouldBe false
  }

}

object DataProcessorSpec {

  /** A descString whose `limit` refers to the loop variable `n`. */
  val limitReferringToN = """{"limit":0,"stateReferences":{"/limit":"n"}}"""

  /** Built as a worker builds it, from `limitReferringToN`. */
  def loopLimitExec(): LoopLimitExec =
    ExecFactory
      .newExecFromJavaClassName(classOf[LoopLimitExec].getName, limitReferringToN)
      .asInstanceOf[LoopLimitExec]

  /** A loop variable's value that counts how often Jackson writes it out, as `State.toJson` does. */
  class JsonWriteCounter {
    var writes = 0

    @JsonValue def json: Int = {
      writes += 1
      1
    }
  }

  /** A setting with one Int property, as Limit's. */
  class LimitSetting extends StateReferencing {
    @JsonProperty var limit: Int = _
  }

  /**
    * Passes the first `limit` tuples, as Limit does, and records each callback the worker makes with
    * the limit and the registered state at that moment. Public, for the factory's reflection.
    */
  class LoopLimitExec(descString: String) extends OperatorExecutor {
    val setting: LimitSetting = objectMapper.readValue(descString, classOf[LimitSetting])
    val calls: ArrayBuffer[(String, Int, Option[State])] = ArrayBuffer()
    var tuplesSeen = 0

    private def record(call: String): Unit = calls += ((call, setting.limit, state))

    override def open(): Unit = record("open")

    override def produceStateOnStart(port: Int): Option[State] = {
      record("start")
      Some(State(Map("start" -> setting.limit)))
    }

    // Transforms the state, so what the worker sends on shows whose return value it is.
    override def processState(incoming: State, port: Int): Option[State] = {
      record("state")
      Some(State(Map("seen" -> incoming.values("n"), "limit" -> setting.limit)))
    }

    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
      tuplesSeen += 1
      if (tuplesSeen <= setting.limit) Iterator.single(tuple) else Iterator.empty
    }

    override def produceStateOnFinish(port: Int): Option[State] = {
      record("finish")
      None
    }
  }
}
