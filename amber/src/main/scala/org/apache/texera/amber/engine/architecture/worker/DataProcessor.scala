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

import com.softwaremill.macwire.wire
import io.grpc.MethodDescriptor
import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple._
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.common.AmberProcessor
import org.apache.texera.amber.engine.architecture.logreplay.ReplayLogManager
import org.apache.texera.amber.engine.architecture.messaginglayer.{
  InputManager,
  OutputManager,
  WorkerTimerService
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.{
  NO_ALIGNMENT,
  PORT_ALIGNMENT
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands._
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_END_CHANNEL
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  MainThreadDelegateMessage
}
import org.apache.texera.amber.engine.architecture.worker.managers.SerializationManager
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerState.{
  COMPLETED,
  READY,
  RUNNING
}
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerStatistics
import org.apache.texera.amber.engine.common.ambermessage._
import org.apache.texera.amber.engine.common.statetransition.WorkerStateManager
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.error.ErrorUtils.{mkConsoleMessage, mkPrintConsoleMessage, safely}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable

class DataProcessor(
    actorId: ActorVirtualIdentity,
    outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit,
    inputMessageQueue: LinkedBlockingQueue[DPInputQueueElement]
) extends AmberProcessor(actorId, outputHandler)
    with Serializable {

  @transient var executor: OperatorExecutor = _

  /** The executor whose loop-variable references `bindStateReferences` has bound. */
  @transient private var executorWithBoundReferences: OperatorExecutor = _

  /**
    * The ids of the state messages this worker has processed since it got its output ports. A
    * message reaches a worker once per path to it, through branches that meet again or several
    * upstream workers, so only the first copy of an id is processed then (see `processInputState`).
    */
  private val processedStateMessages = mutable.HashSet[String]()

  def initTimerService(adaptiveBatchingMonitor: WorkerTimerService): Unit = {
    this.adaptiveBatchingMonitor = adaptiveBatchingMonitor
  }

  @transient var adaptiveBatchingMonitor: WorkerTimerService = _

  // inner dependencies
  private val initializer = new DataProcessorRPCHandlerInitializer(this)
  val pauseManager: PauseManager = wire[PauseManager]
  val stateManager: WorkerStateManager = new WorkerStateManager(actorId)
  val inputManager: InputManager = new InputManager(actorId, inputMessageQueue)
  val outputManager: OutputManager = new OutputManager(actorId, outputGateway)
  val ecmManager: EmbeddedControlMessageManager =
    new EmbeddedControlMessageManager(actorId, inputGateway, inputManager)
  val serializationManager: SerializationManager = new SerializationManager(actorId)

  def getQueuedCredit(channelId: ChannelIdentity): Long = {
    inputGateway.getChannel(channelId).getQueuedCredit
  }

  /**
    * provide API for actor to get stats of this operator
    */
  def collectStatistics(): WorkerStatistics =
    statisticsManager.getStatistics(executor)

  /**
    * `OperatorExecutor.bindStateReferences`, before the executor first sees data or finishes; after
    * that a reference comparison per tuple. An executor installed in its place binds its own.
    */
  def bindStateReferences(): Unit =
    if (executorWithBoundReferences ne executor) {
      executor.bindStateReferences()
      executorWithBoundReferences = executor
    }

  /**
    * process currentInputTuple through executor logic.
    * this function is only called by the DP thread.
    */
  private[this] def processInputTuple(tuple: Tuple): Unit = {
    try {
      val portIdentity: PortIdentity =
        this.inputGateway.getChannel(inputManager.currentChannelId).getPortId
      // The executor sees data: its setting must have its loop variables by now.
      bindStateReferences()
      outputManager.outputIterator.setTupleOutput(
        executor.processTupleMultiPort(
          tuple,
          portIdentity.id
        )
      )

      statisticsManager.increaseInputStatistics(portIdentity, tuple.inMemSize)

    } catch safely {
      case e =>
        // forward input tuple to the user and pause DP thread
        handleExecutorException(e)
    }
  }

  private[this] def processInputState(
      state: State,
      port: Int,
      loopCounter: Long,
      loopStartId: String,
      messageId: String
  ): Unit = {
    // A copy of a message already processed (the same id, arriving on another edge or channel) is
    // not registered, handed to the operator or sent on again. An empty id is a state nobody
    // issued, never a copy. A message processed while the worker has no output port is not
    // recorded. In two-phase region execution (`RegionExecutionManager`) an operator reads its
    // dependee input ports, the ones another of its ports depends on, in a phase of their own,
    // before the worker has any output port (see `OutputManager.isMissingOutputPort`): what it
    // sends then reaches no one, so the copy of the message on its other port is processed again
    // and sent on.
    if (
      messageId.nonEmpty && !outputManager.isMissingOutputPort &&
      !processedStateMessages.add(messageId)
    ) return
    try {
      // Before processState, which then sees the loop variables written into the setting.
      executor.registerState(state, loopCounter)
      val outputState = executor.processState(state, port)
      if (outputState.isDefined) {
        // Carry the incoming loop envelope through unchanged: loop operators
        // are Python-only, so a JVM operator inside a loop body only ever
        // forwards the envelope (the +1/-1 bookkeeping lives in the Python
        // runtime's _process_state_frame). A state sent on unchanged is the
        // same message and keeps its id; a changed one is a new message.
        // A State's map cannot change, so the very state handed over (what
        // the default processState returns) is the same message without
        // serializing either; only another one is compared by its JSON. A
        // value an operator edits in place inside it, such as a byte array,
        // is not seen as a change.
        if ((outputState.get eq state) || sameJson(outputState.get, state)) {
          outputManager.emitState(outputState.get, loopCounter, loopStartId, messageId)
        } else {
          if (loopStartId.nonEmpty) {
            // A change does not replace a loop's message (one stamped with its Loop Start): the
            // message goes on unchanged first, under its id, and the change follows as a new
            // message. The loop body may split before this operator and meet again after it,
            // with the other branch sending the same message on unchanged; arriving there after
            // the change, that branch's copy is one the meeting operator already processed and
            // recorded, so it drops the copy, which cannot undo the change. A worker records an id
            // only once it has output ports (see above), so an operator that reads one input before
            // another processes the copy on its other input again: a change that reached it only
            // through the input it reads first goes no further, and its processState gets the
            // message before the change again after it. The operators downstream run on both
            // messages, so each operator in a row that changes the loop's message doubles the
            // messages after it.
            outputManager.emitState(state, loopCounter, loopStartId, messageId)
          }
          outputManager.emitState(
            outputState.get,
            loopCounter,
            loopStartId,
            UUID.randomUUID().toString
          )
        }
      } else if (loopStartId.nonEmpty) {
        // A loop's message always goes on: body operators forward their loop's messages, so one
        // whose processState returns None for a message stamped with its Loop Start sends that
        // message on unchanged, under its id, and the operators after it and the Loop End still
        // get it. A state without the stamp is not sent on.
        outputManager.emitState(state, loopCounter, loopStartId, messageId)
      }
    } catch safely {
      case e =>
        handleExecutorException(e)
    }
  }

  /**
    * Whether two states write the same JSON, whatever the order of an object's keys: Scala's `==`
    * would also call 2 and 2.0 equal, which their JSON tells apart.
    */
  private[this] def sameJson(a: State, b: State): Boolean =
    objectMapper.readTree(a.toJson) == objectMapper.readTree(b.toJson)

  /** transfer one tuple from iterator to downstream.
    * this function is only called by the DP thread
    */
  private[this] def outputOneTuple(): Unit = {
    adaptiveBatchingMonitor.startAdaptiveBatching()
    var out: (TupleLike, Option[PortIdentity]) = null
    try {
      out = outputManager.outputIterator.next()
    } catch safely {
      case e =>
        // invalidate current output tuple
        out = null
        // also invalidate outputIterator
        outputManager.outputIterator.setTupleOutput(Iterator.empty)
        // forward input tuple to the user and pause DP thread
        handleExecutorException(e)
    }
    if (out == null) return

    val (outputTuple, outputPortOpt) = out

    if (outputTuple == null) return
    outputTuple match {
      case FinalizeExecutor() =>
        sendECMToDataChannels(METHOD_END_CHANNEL, PORT_ALIGNMENT)
        // Surface non-fatal warnings the executor accumulated (e.g. rows a scan
        // skipped) as console messages. Unlike handleExecutorException, this must
        // not pause the run — the messages are emitted and processing continues.
        executor.getWarnings.foreach { warning =>
          asyncRPCClient.coordinatorInterface.consoleMessageTriggered(
            ConsoleMessageTriggeredRequest(mkPrintConsoleMessage(actorId, warning)),
            asyncRPCClient.mkContext(COORDINATOR)
          )
        }
        // Send Completed signal to worker actor.
        executor.close()
        adaptiveBatchingMonitor.stopAdaptiveBatching()
        stateManager.transitTo(COMPLETED)
        logger.debug(
          s"$executor completed, # of input ports = ${inputManager.getAllPorts.size}, " +
            s"input tuple count = ${statisticsManager.getInputTupleCount}, " +
            s"output tuple count = ${statisticsManager.getOutputTupleCount}"
        )
        asyncRPCClient.coordinatorInterface.workerExecutionCompleted(
          EmptyRequest(),
          asyncRPCClient.mkContext(COORDINATOR)
        )
      case FinalizePort(portId, input) =>
        if (!input) {
          outputManager.closeOutputStorageWriterIfNeeded(portId)
        }
        asyncRPCClient.coordinatorInterface.portCompleted(
          PortCompletedRequest(portId, input),
          asyncRPCClient.mkContext(COORDINATOR)
        )
      case schemaEnforceable: SchemaEnforceable =>
        val portIdentity = outputPortOpt.getOrElse(outputManager.getSingleOutputPortIdentity)
        val tuple = schemaEnforceable.enforceSchema(outputManager.getPort(portIdentity).schema)
        statisticsManager.increaseOutputStatistics(portIdentity, tuple.inMemSize)
        outputManager.passTupleToDownstream(tuple, outputPortOpt)
        outputManager.saveTupleToStorageIfNeeded(tuple, outputPortOpt)

      case other => // skip for now
    }
  }

  def continueDataProcessing(): Unit = {
    val dataProcessingStartTime = System.nanoTime()
    if (outputManager.hasUnfinishedOutput) {
      outputOneTuple()
    } else {
      processInputTuple(inputManager.getNextTuple)
    }
    statisticsManager.increaseDataProcessingTime(System.nanoTime() - dataProcessingStartTime)
  }

  def processDataPayload(
      channelId: ChannelIdentity,
      dataPayload: DataPayload
  ): Unit = {
    val dataProcessingStartTime = System.nanoTime()
    val portId = this.inputGateway.getChannel(channelId).getPortId
    dataPayload match {
      case DataFrame(tuples) =>
        stateManager.conditionalTransitTo(
          READY,
          RUNNING,
          () => {
            val (state, stateVersion) = stateManager.getStateWithVersion
            asyncRPCClient.coordinatorInterface.workerStateUpdated(
              WorkerStateUpdatedRequest(state, stateVersion),
              asyncRPCClient.mkContext(COORDINATOR)
            )
          }
        )
        inputManager.initBatch(channelId, tuples)
        processInputTuple(inputManager.getNextTuple)
      case StateFrame(state, loopCounter, loopStartId, messageId) =>
        processInputState(state, portId.id, loopCounter, loopStartId, messageId)
    }
    statisticsManager.increaseDataProcessingTime(System.nanoTime() - dataProcessingStartTime)
  }

  def processECM(
      channelId: ChannelIdentity,
      ecm: EmbeddedControlMessage,
      logManager: ReplayLogManager
  ): Unit = {
    inputManager.currentChannelId = channelId
    val command = ecm.commandMapping.get(actorId.name)
    logger.debug(s"receive ECM from $channelId, id = ${ecm.id}, cmd = $command")
    if (ecm.ecmType != NO_ALIGNMENT) {
      pauseManager.pauseInputChannel(ECMPause(ecm.id), List(channelId))
    }
    if (ecmManager.isECMAligned(channelId, ecm)) {
      logManager.markAsReplayDestination(ecm.id)
      // invoke the control command carried with the ECM
      logger.debug(s"process ECM from $channelId, id = ${ecm.id}, cmd = $command")
      if (command.isDefined) {
        // The reply must go back to the actor that originated the invocation
        // (recorded in command.context.sender), not to channelId.fromWorkerId.
        // For ECM-embedded commands those differ: channelId is the data
        // channel between two workers, while the originator is typically the
        // coordinator. Fall back to the channel sender when the context is
        // unset (e.g. unit-test inputs).
        val ctx = command.get.context
        val replyTo =
          if (ctx.sender.name.nonEmpty) ctx.sender else channelId.fromWorkerId
        asyncRPCServer.receive(command.get, replyTo)
      }
      // if this worker is not the final destination of the ECM, pass it downstream
      val downstreamChannelsInScope = ecm.scope.filter(_.fromWorkerId == actorId).toSet
      if (downstreamChannelsInScope.nonEmpty) {
        outputManager.flush(Some(downstreamChannelsInScope))
        outputGateway.getActiveChannels.foreach { activeChannelId =>
          if (downstreamChannelsInScope.contains(activeChannelId)) {
            logger.debug(
              s"send ECM to $activeChannelId, id = ${ecm.id}, cmd = $command"
            )
            outputGateway.sendTo(activeChannelId, ecm)
          }
        }
      }
      // unblock input channels
      if (ecm.ecmType != NO_ALIGNMENT) {
        pauseManager.resume(ECMPause(ecm.id))
      }
    }
  }

  def sendECMToDataChannels(
      method: MethodDescriptor[EmptyRequest, EmptyReturn],
      alignment: EmbeddedControlMessageType
  ): Unit = {
    outputManager.flush()
    outputGateway.getActiveChannels
      .filter(!_.isControl)
      .foreach { activeChannelId =>
        asyncRPCClient.sendECMToChannel(
          EmbeddedControlMessageIdentity(method.getBareMethodName),
          alignment,
          Set(),
          Map(
            activeChannelId.toWorkerId.name ->
              ControlInvocation(
                method.getBareMethodName,
                EmptyRequest(),
                AsyncRPCContext(ActorVirtualIdentity(""), ActorVirtualIdentity("")),
                -1
              )
          ),
          activeChannelId
        )
      }
  }

  def handleExecutorException(e: Throwable): Unit = {
    asyncRPCClient.coordinatorInterface.consoleMessageTriggered(
      ConsoleMessageTriggeredRequest(mkConsoleMessage(actorId, e)),
      asyncRPCClient.mkContext(COORDINATOR)
    )
    logger.warn(e.getLocalizedMessage + "\n" + e.getStackTrace.mkString("\n"))
    // invoke a pause in-place
    pauseManager.pause(OperatorLogicPause)
  }
}
