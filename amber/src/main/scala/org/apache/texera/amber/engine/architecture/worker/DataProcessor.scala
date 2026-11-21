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
  OperatorExecutor,
  SourceOperatorExecutor
}
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
import org.apache.texera.amber.util.ArrowUtils
import org.apache.arrow.vector.VectorSchemaRoot

import java.util.concurrent.LinkedBlockingQueue

class DataProcessor(
    actorId: ActorVirtualIdentity,
    outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit,
    inputMessageQueue: LinkedBlockingQueue[DPInputQueueElement]
) extends AmberProcessor(actorId, outputHandler)
    with Serializable {

  @transient var executor: OperatorExecutor = _

  /** The executor whose loop-variable references `bindStateReferences` has bound. */
  @transient private var executorWithBoundReferences: OperatorExecutor = _

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
  // Start processing one data batch: announce the worker is running, load the
  // batch, and process its first tuple (the DP loop drains the rest). Shared by the
  // DataFrame and VectorFrame input paths.
  private[this] def startInputBatch(
      channelId: ChannelIdentity,
      tuples: Array[Tuple]
  ): Unit = {
    transitToRunning()
    inputManager.initBatch(channelId, tuples)
    processInputTuple(inputManager.getNextTuple)
  }

  // Announce the worker is running (READY -> RUNNING, once). Shared by the row and
  // columnar input paths.
  private[this] def transitToRunning(): Unit = {
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
  }

  // An Arrow batch arrived. A ColumnarOperatorExecutor consumes it directly; otherwise (or if it
  // declines) decode to tuples and process as a DataFrame.
  private[this] def processVectorFrame(
      channelId: ChannelIdentity,
      portId: PortIdentity,
      arrowIpcBytes: Array[Byte],
      rowCount: Int
  ): Unit = {
    val columnarExecutor = executor match {
      case c: ColumnarOperatorExecutor => c
      case _                           => null
    }
    if (columnarExecutor == null) {
      startInputBatch(channelId, ArrowUtils.deserializeTuples(arrowIpcBytes))
      return
    }
    transitToRunning()
    inputManager.currentChannelId = channelId
    // The executor sees data: bind its loop variables first (parity with the row path).
    bindStateReferences()
    try {
      columnarExecutor.processColumnarBatch(arrowIpcBytes, portId.id) match {
        case ColumnarResult.Unsupported =>
          // The executor declined this batch: fall back to the row path.
          startInputBatch(channelId, ArrowUtils.deserializeTuples(arrowIpcBytes))
        case consumed =>
          routeConsumedResult(channelId, portId, rowCount, arrowIpcBytes.length.toLong, consumed)
      }
    } catch safely {
      case e => handleExecutorException(e)
    }
  }

  // A live Arrow root arrived by reference (this worker owns and closes it). Like processVectorFrame
  // but with no IPC decode: a ColumnarOperatorExecutor reads it directly, else decode to tuples.
  private[this] def processLocalColumnarRoot(
      channelId: ChannelIdentity,
      portId: PortIdentity,
      root: VectorSchemaRoot,
      rowCount: Int,
      inMemSize: Long
  ): Unit = {
    val columnarExecutor = executor match {
      case c: ColumnarOperatorExecutor => c
      case _                           => null
    }
    try {
      if (columnarExecutor == null) {
        startInputBatch(channelId, ArrowUtils.tuplesOfRoot(root))
      } else {
        transitToRunning()
        inputManager.currentChannelId = channelId
        bindStateReferences()
        try {
          columnarExecutor.processColumnarRoot(root, portId.id) match {
            case ColumnarResult.Unsupported =>
              startInputBatch(channelId, ArrowUtils.tuplesOfRoot(root))
            case consumed =>
              routeConsumedResult(channelId, portId, rowCount, inMemSize, consumed)
          }
        } catch safely {
          case e => handleExecutorException(e)
        }
      }
    } finally root.close() // the receiver owns the handed-off root
  }

  // After an executor consumes a whole Arrow batch: count input stats once, then route the result
  // (Emit ships a root, EmitRows installs rows, Consumed emits nothing).
  private[this] def routeConsumedResult(
      channelId: ChannelIdentity,
      portId: PortIdentity,
      rowCount: Int,
      inputByteSize: Long,
      consumed: ColumnarResult
  ): Unit = {
    inputManager.initBatch(channelId, Array.empty)
    statisticsManager.increaseInputStatistics(portId, rowCount.toLong, inputByteSize)
    consumed match {
      case ColumnarResult.EmitRows(rows) => outputManager.outputIterator.setTupleOutput(rows)
      case ColumnarResult.Emit(root)     => emitProducedRoot(root)
      case _                             => // Consumed: a blocking operator accumulated the batch
    }
  }

  // Counts the Arrow batches a columnar source emitted, for the console summary on finish.
  @transient private var columnarBatchCount = 0
  @transient private var columnarRowCount = 0L

  // Ship a produced Arrow root downstream in the cheapest shape: a live root to a local receiver
  // (not closed here, ownership transfers), Arrow bytes to a remote one, or decoded rows.
  private[this] def emitProducedRoot(outRoot: VectorSchemaRoot): Unit = {
    var retained = false
    try {
      val rc = outRoot.getRowCount
      outputManager.tryEmitLocalColumnarRoot(outRoot, None) match {
        case Some((pid, sz)) =>
          retained = true
          statisticsManager.increaseOutputStatistics(pid, rc.toLong, sz)
        case None =>
          outputManager.tryEmitColumnarBatch(outRoot, None) match {
            case Some((pid, bytes)) =>
              statisticsManager.increaseOutputStatistics(pid, rc.toLong, bytes)
            case None =>
              val tuples = ArrowUtils.tuplesOfRoot(outRoot)
              outputManager.outputIterator.setTupleOutput(
                tuples.iterator.map(t => (t: TupleLike, None: Option[PortIdentity]))
              )
          }
      }
    } finally if (!retained) outRoot.close()
  }

  // A columnar-eligible source's Arrow batches, each wrapped as a ColumnarBatchMarker for the DP
  // loop to ship via emitProducedRoot (no Tuple built). None means endChannel uses the row path.
  def sourceColumnarOutput(): Option[Iterator[(TupleLike, Option[PortIdentity])]] = {
    executor match {
      case src: SourceOperatorExecutor if outputManager.columnarSourceEmitEligible =>
        val schema =
          outputManager.getPort(outputManager.getSingleOutputPortIdentity).schema
        src
          .produceColumnarBatch(schema, outputManager.outputBatchSize)
          .map(_.map(root => (ColumnarBatchMarker(root): TupleLike, Option.empty[PortIdentity])))
      case _ => None
    }
  }

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
      loopStartId: String
  ): Unit = {
    try {
      // Before processState, which then sees the loop variables written into the setting.
      executor.registerState(state, loopCounter)
      val outputState = executor.processState(state, port)
      if (outputState.isDefined) {
        // Carry the incoming loop envelope through unchanged: loop operators
        // are Python-only, so a JVM operator inside a loop body only ever
        // forwards the envelope (the +1/-1 bookkeeping lives in the Python
        // runtime's _process_state_frame).
        outputManager.emitState(outputState.get, loopCounter, loopStartId)
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
        // Surface non-fatal warnings the executor accumulated (e.g. rows a scan
        // skipped) as console messages. Unlike handleExecutorException, this must
        // not pause the run — the messages are emitted and processing continues.
        executor.getWarnings.foreach { warning =>
          asyncRPCClient.coordinatorInterface.consoleMessageTriggered(
            ConsoleMessageTriggeredRequest(mkPrintConsoleMessage(actorId, warning)),
            asyncRPCClient.mkContext(COORDINATOR)
          )
        }
        // If this worker emitted Arrow batches (columnar source), report the batch/row count.
        if (columnarBatchCount > 0) {
          asyncRPCClient.coordinatorInterface.consoleMessageTriggered(
            ConsoleMessageTriggeredRequest(
              mkPrintConsoleMessage(
                actorId,
                s"[columnar] emitted $columnarBatchCount Arrow column batch(es), " +
                  s"$columnarRowCount rows, with 0 row objects built"
              )
            ),
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
      case ColumnarBatchMarker(root) =>
        // A columnar source's Arrow batch: ship it downstream via emitProducedRoot, no Tuple built.
        columnarBatchCount += 1
        columnarRowCount += root.getRowCount
        emitProducedRoot(root)

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
        startInputBatch(channelId, tuples)
      case VectorFrame(arrowIpcBytes, rowCount, _) =>
        // Columnar wire: the batch arrived as Arrow IPC bytes. A
        // ColumnarOperatorExecutor consumes the Arrow columns directly (no per-row
        // decode); any other operator, or one that declines the batch, decodes it
        // back to tuples and processes it exactly as a DataFrame.
        processVectorFrame(channelId, portId, arrowIpcBytes, rowCount)
      case batch: LocalColumnarBatch =>
        // Local zero-copy wire: a live Arrow root arrived by reference from a co-located
        // sender. This worker owns and closes it.
        processLocalColumnarRoot(channelId, portId, batch.root, batch.rowCount, batch.inMemSize)
      case StateFrame(state, loopCounter, loopStartId) =>
        processInputState(state, portId.id, loopCounter, loopStartId)
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
