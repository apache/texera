package edu.uci.ics.amber.engine.architecture.worker

import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.common.AmberProcessor
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ConsoleMessageHandler.ConsoleMessageTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PortCompletedHandler.PortCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerStateUpdatedHandler.WorkerStateUpdated
import edu.uci.ics.amber.engine.architecture.logreplay.ReplayLogManager
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  InputManager,
  OutputManager,
  WorkerTimerService
}
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import edu.uci.ics.amber.engine.architecture.worker.managers.SerializationManager
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{
  COMPLETED,
  READY,
  RUNNING
}
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerStatistics
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.tuple.amber.{
  FinalizeExecutor,
  FinalizePort,
  SchemaEnforceable,
  TupleLike
}
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.amber.error.ErrorUtils.{mkConsoleMessage, safely}
import edu.uci.ics.texera.workflow.common.EndOfUpstream
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class DataProcessor(
    actorId: ActorVirtualIdentity,
    outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit
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
  val inputManager: InputManager = new InputManager(actorId)
  val outputManager: OutputManager = new OutputManager(actorId, outputGateway)
  val channelMarkerManager: ChannelMarkerManager = new ChannelMarkerManager(actorId, inputGateway)
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

      statisticsManager.increaseInputTupleCount(portIdentity)

    } catch safely {
      case e =>
        // forward input tuple to the user and pause DP thread
        handleExecutorException(e)
    }
  }

  /**
    * process end of an input port with Executor.onFinish().
    * this function is only called by the DP thread.
    */
  private[this] def processEndOfUpstream(portId: Int): Unit = {
    try {
      outputManager.outputIterator.setTupleOutput(
        executor.onFinishMultiPort(portId)
      )
    } catch safely {
      case e =>
        // forward input tuple to the user and pause DP thread
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
        outputManager.emitMarker(EndOfUpstream())
        // Send Completed signal to worker actor.
        executor.close()
        adaptiveBatchingMonitor.stopAdaptiveBatching()
        stateManager.transitTo(COMPLETED)
        logger.info(
          s"$executor completed, # of input ports = ${inputManager.getAllPorts.size}, " +
            s"input tuple count = ${statisticsManager.getInputTupleCount}, " +
            s"output tuple count = ${statisticsManager.getOutputTupleCount}"
        )
        asyncRPCClient.send(WorkerExecutionCompleted(), CONTROLLER)
      case FinalizePort(portId, input) =>
        asyncRPCClient.send(PortCompleted(portId, input), CONTROLLER)
      case schemaEnforceable: SchemaEnforceable =>
        if (outputPortOpt.isEmpty) {
          statisticsManager.increaseOutputTupleCount(outputManager.getSingleOutputPortIdentity)
        } else {
          statisticsManager.increaseOutputTupleCount(outputPortOpt.get)
        }
        outputManager.passTupleToDownstream(schemaEnforceable, outputPortOpt)

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
            asyncRPCClient.send(
              WorkerStateUpdated(stateManager.getCurrentState),
              CONTROLLER
            )
          }
        )
        inputManager.initBatch(channelId, tuples)
        processInputTuple(inputManager.getNextTuple)
      case MarkerFrame(marker) =>
        marker match {
          case EndOfUpstream() =>
            this.inputManager.getPort(portId).channels(channelId) = true
            if (inputManager.isPortCompleted(portId)) {
              inputManager.initBatch(channelId, Array.empty)
              processEndOfUpstream(portId.id)
              outputManager.outputIterator.appendSpecialTupleToEnd(
                FinalizePort(portId, input = true)
              )
            }
            if (inputManager.getAllPorts.forall(portId => inputManager.isPortCompleted(portId))) {
              // assuming all the output ports finalize after all input ports are finalized.
              outputManager.finalizeOutput()
            }
        }
    }
    statisticsManager.increaseDataProcessingTime(System.nanoTime() - dataProcessingStartTime)
  }

  def processChannelMarker(
      channelId: ChannelIdentity,
      marker: ChannelMarkerPayload,
      logManager: ReplayLogManager
  ): Unit = {
    val markerId = marker.id
    val command = marker.commandMapping.get(actorId)
    logger.info(s"receive marker from $channelId, id = ${marker.id}, cmd = ${command}")
    if (marker.markerType == RequireAlignment) {
      pauseManager.pauseInputChannel(EpochMarkerPause(markerId), List(channelId))
    }
    if (channelMarkerManager.isMarkerAligned(channelId, marker)) {
      logManager.markAsReplayDestination(markerId)
      // invoke the control command carried with the epoch marker
      logger.info(s"process marker from $channelId, id = ${marker.id}, cmd = ${command}")
      if (command.isDefined) {
        asyncRPCServer.receive(command.get, channelId.fromWorkerId)
      }
      // if this worker is not the final destination of the marker, pass it downstream
      val downstreamChannelsInScope = marker.scope.filter(_.fromWorkerId == actorId)
      if (downstreamChannelsInScope.nonEmpty) {
        outputManager.flush(Some(downstreamChannelsInScope))
        outputGateway.getActiveChannels.foreach { activeChannelId =>
          if (downstreamChannelsInScope.contains(activeChannelId)) {
            logger.info(
              s"send marker to $activeChannelId, id = ${marker.id}, cmd = ${command}"
            )
            outputGateway.sendTo(activeChannelId, marker)
          }
        }
      }
      // unblock input channels
      if (marker.markerType == RequireAlignment) {
        pauseManager.resume(EpochMarkerPause(markerId))
      }
    }
  }

  private[this] def handleExecutorException(e: Throwable): Unit = {
    asyncRPCClient.send(
      ConsoleMessageTriggered(mkConsoleMessage(actorId, e)),
      CONTROLLER
    )
    logger.warn(e.getLocalizedMessage + "\n" + e.getStackTrace.mkString("\n"))
    // invoke a pause in-place
    asyncRPCServer.execute(PauseWorker(), SELF)
  }
}
