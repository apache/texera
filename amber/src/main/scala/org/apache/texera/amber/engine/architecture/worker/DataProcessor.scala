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
import org.apache.texera.amber.core.executor.{
  ColumnarOperatorExecutor,
  ColumnarResult,
  OperatorExecutor
}
import org.apache.texera.amber.engine.architecture.sendsemantics.partitioners.NetworkOutputBuffer
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
import org.apache.texera.amber.error.ErrorUtils.{mkConsoleMessage, safely}
import org.apache.texera.amber.util.ArrowUtils

import java.util.concurrent.LinkedBlockingQueue

class DataProcessor(
    actorId: ActorVirtualIdentity,
    outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit,
    inputMessageQueue: LinkedBlockingQueue[DPInputQueueElement]
) extends AmberProcessor(actorId, outputHandler)
    with Serializable {

  @transient var executor: OperatorExecutor = _

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
    * process currentInputTuple through executor logic.
    * this function is only called by the DP thread.
    */
  private[this] def processInputTuple(tuple: Tuple): Unit = {
    try {
      val portIdentity: PortIdentity =
        this.inputGateway.getChannel(inputManager.currentChannelId).getPortId
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

  private[this] def processInputState(state: State, port: Int): Unit = {
    try {
      val outputState = executor.processState(state, port)
      if (outputState.isDefined) {
        outputManager.emitState(outputState.get)
      }
    } catch safely {
      case e =>
        handleExecutorException(e)
    }
  }

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
        // Send Completed signal to worker actor.
        executor.close()
        outputManager.closeColumnarResources()
        adaptiveBatchingMonitor.stopAdaptiveBatching()
        stateManager.transitTo(COMPLETED)
        logger.info(
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
    if (outputManager.hasUnfinishedColumnarOutput) {
      outputManager.emitOneColumnarBatch()
    } else if (outputManager.hasUnfinishedOutput) {
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
        processTupleBatch(channelId, portId, tuples)
      case ColumnarFrame(bytes, _, _) =>
        executor match {
          // Native-Arrow path: consume the batch directly, emit a filtered batch.
          case c: ColumnarOperatorExecutor if NetworkOutputBuffer.columnarWire =>
            c.processColumnarBatch(bytes, portId.id) match {
              case ColumnarResult.Emit(result) =>
                logColumnarModeOnce(active = true, "")
                statisticsManager.increaseInputStatistics(portId, bytes.length.toLong)
                outputManager.setColumnarOutput(Iterator.single(result))
              case ColumnarResult.EmitRows(rows) =>
                logColumnarModeOnce(active = true, "")
                statisticsManager.increaseInputStatistics(portId, bytes.length.toLong)
                outputManager.outputIterator.setTupleOutput(rows)
              case ColumnarResult.Consumed =>
                logColumnarModeOnce(active = true, "")
                statisticsManager.increaseInputStatistics(portId, bytes.length.toLong)
              case ColumnarResult.Unsupported =>
                logColumnarModeOnce(active = false, "operator has no native columnar path for this batch")
                processTupleBatch(channelId, portId, ArrowUtils.deserializeTuples(bytes))
            }
          case _ =>
            processTupleBatch(channelId, portId, ArrowUtils.deserializeTuples(bytes))
        }
      case StateFrame(state) =>
        processInputState(state, portId.id)
    }
    statisticsManager.increaseDataProcessingTime(System.nanoTime() - dataProcessingStartTime)
  }

  private def processTupleBatch(
      channelId: ChannelIdentity,
      portId: PortIdentity,
      tuples: Array[Tuple]
  ): Unit = {
    stateManager.conditionalTransitTo(
      READY,
      RUNNING,
      () => {
        asyncRPCClient.coordinatorInterface.workerStateUpdated(
          WorkerStateUpdatedRequest(stateManager.getCurrentState),
          asyncRPCClient.mkContext(COORDINATOR)
        )
      }
    )
    inputManager.initBatch(channelId, tuples)
    // Whole-batch path if the executor supports it, else per-tuple; errors fall back.
    val vectorized: Option[Iterator[(TupleLike, Option[PortIdentity])]] =
      try executor.processBatchMultiPort(tuples, portId.id)
      catch safely { case _ => None }
    vectorized match {
      case Some(outputIter) =>
        var i = 0
        while (i < tuples.length) {
          statisticsManager.increaseInputStatistics(portId, tuples(i).inMemSize)
          i += 1
        }
        inputManager.skipToEnd()
        outputManager.outputIterator.setTupleOutput(outputIter)
      case None =>
        processInputTuple(inputManager.getNextTuple)
    }
  }

  def processECM(
      channelId: ChannelIdentity,
      ecm: EmbeddedControlMessage,
      logManager: ReplayLogManager
  ): Unit = {
    inputManager.currentChannelId = channelId
    val command = ecm.commandMapping.get(actorId.name)
    logger.info(s"receive ECM from $channelId, id = ${ecm.id}, cmd = $command")
    if (ecm.ecmType != NO_ALIGNMENT) {
      pauseManager.pauseInputChannel(ECMPause(ecm.id), List(channelId))
    }
    if (ecmManager.isECMAligned(channelId, ecm)) {
      logManager.markAsReplayDestination(ecm.id)
      // invoke the control command carried with the ECM
      logger.info(s"process ECM from $channelId, id = ${ecm.id}, cmd = $command")
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
            logger.info(
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

  // Log the columnar-wire decision once per worker so a silent fall back to the
  // row path is visible in the logs.
  @transient private var columnarModeLogged = false
  private[architecture] def logColumnarModeOnce(active: Boolean, reason: String): Unit = {
    if (!columnarModeLogged) {
      columnarModeLogged = true
      if (active) logger.info(s"columnar wire active for $executor")
      else logger.info(s"columnar wire requested but using row path for $executor: $reason")
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
