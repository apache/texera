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

import com.fasterxml.jackson.annotation.JsonProperty
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
    sent.map(_.payload).collect { case StateFrame(state, _, _) => state }.toList

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

  "data processor" should "fail a state message from the same loop that changes a loop variable" in {
    val (dp, exec, sent) = initializedWith(limitReferringToN)
    openExecutor(dp)
    dp.processDataPayload(senderChannel, StateFrame(State(Map("n" -> 2L))))
    dp.processDataPayload(senderChannel, StateFrame(State(Map("n" -> 3L))))

    // The operator never sees the changed state.
    assert(exec.calls.map(_._1).toList == List("open", "state"))
    assert(
      consoleTitles(sent) == List(
        new IllegalStateException(
          "loop variable n got two different values in one iteration, 2 and 3: a loop's " +
            "variables must not change inside its body"
        ).toString
      )
    )
    dp.pauseManager.isPaused shouldBe true
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
