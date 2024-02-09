package edu.uci.ics.amber.engine.architecture.worker

import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.common.AmberProcessor
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ConsoleMessageHandler.ConsoleMessageTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PortCompletedHandler.PortCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionStartedHandler.WorkerStateUpdated
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{
  OpExecInitInfoWithCode,
  OpExecInitInfoWithFunc
}
import edu.uci.ics.amber.engine.architecture.logreplay.ReplayLogManager
import edu.uci.ics.amber.engine.architecture.messaginglayer.{OutputManager, WorkerTimerService}
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig
import edu.uci.ics.amber.engine.architecture.worker.DataProcessor.{
  DPOutputIterator,
  FinalizeOperator,
  FinalizePort
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{
  COMPLETED,
  PAUSED,
  READY,
  RUNNING
}
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerStatistics
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.amber.engine.common.{
  IOperatorExecutor,
  ISinkOperatorExecutor,
  InputExhausted,
  VirtualIdentityUtils
}
import edu.uci.ics.amber.error.ErrorUtils.{mkConsoleMessage, safely}

import scala.collection.mutable

object DataProcessor {

  class SpecialDataTuple extends ITuple {
    override def length: Int = 0

    override def get(i: Int): Any = null

    override def toArray(): Array[Any] = Array.empty

    override def inMemSize: Long = 0
  }
  case class FinalizePort(portId: PortIdentity, input: Boolean) extends SpecialDataTuple
  case class FinalizeOperator() extends SpecialDataTuple

  class DPOutputIterator extends Iterator[(ITuple, Option[PortIdentity])] {
    val queue = new mutable.Queue[(ITuple, Option[PortIdentity])]
    @transient var outputIter: Iterator[(ITuple, Option[PortIdentity])] = Iterator.empty

    def setTupleOutput(outputIter: Iterator[(ITuple, Option[PortIdentity])]): Unit = {
      if (outputIter != null) {
        this.outputIter = outputIter
      } else {
        this.outputIter = Iterator.empty
      }
    }

    override def hasNext: Boolean = outputIter.hasNext || queue.nonEmpty

    override def next(): (ITuple, Option[PortIdentity]) = {
      if (outputIter.hasNext) {
        outputIter.next()
      } else {
        queue.dequeue()
      }
    }

    def appendSpecialTupleToEnd(tuple: ITuple): Unit = {
      queue.enqueue((tuple, None))
    }
  }

}

class DataProcessor(
    actorId: ActorVirtualIdentity,
    outputHandler: WorkflowFIFOMessage => Unit
) extends AmberProcessor(actorId, outputHandler)
    with Serializable {

  @transient var workerIdx: Int = 0
  @transient var physicalOp: PhysicalOp = _
  @transient var operatorConfig: OperatorConfig = _
  @transient var operator: IOperatorExecutor = _

  def initOperator(
      workerIdx: Int,
      physicalOp: PhysicalOp,
      operatorConfig: OperatorConfig,
      currentOutputIterator: Iterator[(ITuple, Option[PortIdentity])]
  ): Unit = {
    this.workerIdx = workerIdx
    this.operator = physicalOp.opExecInitInfo match {
      case OpExecInitInfoWithCode(codeGen) => ??? // TODO: compile and load java/scala operator here
      case OpExecInitInfoWithFunc(opGen) =>
        opGen(workerIdx, physicalOp, operatorConfig)
    }
    this.operatorConfig = operatorConfig
    this.physicalOp = physicalOp

    this.outputIterator.setTupleOutput(currentOutputIterator)
  }

  var outputIterator: DPOutputIterator = new DPOutputIterator()

  var inputBatch: Array[ITuple] = _
  var currentInputIdx: Int = -1
  var currentChannelId: ChannelIdentity = _

  def initTimerService(adaptiveBatchingMonitor: WorkerTimerService): Unit = {
    this.adaptiveBatchingMonitor = adaptiveBatchingMonitor
  }

  @transient var adaptiveBatchingMonitor: WorkerTimerService = _

  def getOperatorId: PhysicalOpIdentity = VirtualIdentityUtils.getPhysicalOpId(actorId)
  def getWorkerIndex: Int = VirtualIdentityUtils.getWorkerIndex(actorId)

  // inner dependencies
  private val initializer = new DataProcessorRPCHandlerInitializer(this)
  // 1. pause manager
  val pauseManager: PauseManager = wire[PauseManager]
  // 2. breakpoint manager
  val breakpointManager: BreakpointManager = new BreakpointManager(asyncRPCClient)
  // 3. state manager
  val stateManager: WorkerStateManager = new WorkerStateManager()
  // 4. batch producer
  val outputManager: OutputManager = new OutputManager(actorId, outputGateway)
  // 5. epoch manager
  val channelMarkerManager: ChannelMarkerManager = new ChannelMarkerManager(actorId, inputGateway)

  // dp thread stats:
  protected var inputTupleCount = 0L
  protected var outputTupleCount = 0L
  var startTime = 0L
  var totalExecutionTime = 0L
  var dataProcessingTime = 0L

  def getQueuedCredit(channelId: ChannelIdentity): Long = {
    inputGateway.getChannel(channelId).getQueuedCredit
  }
  def onInterrupt(): Unit = {
    adaptiveBatchingMonitor.pauseAdaptiveBatching()
  }

  def onContinue(): Unit = {
    adaptiveBatchingMonitor.resumeAdaptiveBatching()
  }

  /** provide API for actor to get stats of this operator
    *
    * @return (input tuple count, output tuple count)
    */
  def collectStatistics(): WorkerStatistics = {
    // sink operator doesn't output to downstream so internal count is 0
    // but for user-friendliness we show its input count as output count
    val displayOut = operator match {
      case sink: ISinkOperatorExecutor =>
        inputTupleCount
      case _ =>
        outputTupleCount
    }
    WorkerStatistics(
      stateManager.getCurrentState,
      inputTupleCount,
      displayOut,
      dataProcessingTime,
      controlProcessingTime,
      totalExecutionTime - dataProcessingTime - controlProcessingTime
    )
  }

  /** process currentInputTuple through operator logic.
    * this function is only called by the DP thread
    *
    * @return an iterator of output tuples
    */
  private[this] def processInputTuple(tuple: Either[ITuple, InputExhausted]): Unit = {
    try {
      outputIterator.setTupleOutput(
        operator.processTuple(
          tuple,
          this.inputGateway.getChannel(currentChannelId).getPortId.id,
          pauseManager,
          asyncRPCClient
        )
      )
      if (tuple.isLeft) {
        inputTupleCount += 1
      }
    } catch safely {
      case e =>
        // forward input tuple to the user and pause DP thread
        handleOperatorException(e)
    }
  }

  /** transfer one tuple from iterator to downstream.
    * this function is only called by the DP thread
    */
  private[this] def outputOneTuple(): Unit = {
    adaptiveBatchingMonitor.startAdaptiveBatching()
    var out: (ITuple, Option[PortIdentity]) = null
    try {
      out = outputIterator.next()
    } catch safely {
      case e =>
        // invalidate current output tuple
        out = null
        // also invalidate outputIterator
        outputIterator.setTupleOutput(Iterator.empty)
        // forward input tuple to the user and pause DP thread
        handleOperatorException(e)
    }
    if (out == null) return

    val (outputTuple, outputPortOpt) = out

    if (outputTuple == null) return

    outputTuple match {
      case FinalizeOperator() =>
        outputManager.emitEndOfUpstream()
        // Send Completed signal to worker actor.
        logger.info(
          s"$operator completed, outputted = $outputTupleCount"
        )
        operator.close() // close operator
        adaptiveBatchingMonitor.stopAdaptiveBatching()
        stateManager.transitTo(COMPLETED)
        asyncRPCClient.send(WorkerExecutionCompleted(), CONTROLLER)
      case FinalizePort(portId, input) =>
        asyncRPCClient.send(PortCompleted(portId, input), CONTROLLER)
      case _ =>
        if (breakpointManager.evaluateTuple(outputTuple)) {
          pauseManager.pause(UserPause)
          adaptiveBatchingMonitor.pauseAdaptiveBatching()
          stateManager.transitTo(PAUSED)
        } else {
          outputTupleCount += 1
          val outLinks = physicalOp.getOutputLinks(outputPortOpt)
          outLinks.foreach(link => outputManager.passTupleToDownstream(outputTuple, link))
        }
    }
  }

  def hasUnfinishedInput: Boolean = inputBatch != null && currentInputIdx + 1 < inputBatch.length

  def hasUnfinishedOutput: Boolean = outputIterator.hasNext

  def continueDataProcessing(): Unit = {
    val dataProcessingStartTime = System.nanoTime()
    if (hasUnfinishedOutput) {
      outputOneTuple()
    } else {
      currentInputIdx += 1
      processInputTuple(Left(inputBatch(currentInputIdx)))
    }
    dataProcessingTime += (System.nanoTime() - dataProcessingStartTime)
  }

  private[this] def initBatch(channelId: ChannelIdentity, batch: Array[ITuple]): Unit = {
    currentChannelId = channelId
    inputBatch = batch
    currentInputIdx = 0
  }

  def getCurrentInputTuple: ITuple = {
    if (inputBatch == null) {
      null
    } else if (inputBatch.isEmpty) {
      ITuple("Input Exhausted")
    } else {
      inputBatch(currentInputIdx)
    }
  }

  def processDataPayload(
      channelId: ChannelIdentity,
      dataPayload: DataPayload
  ): Unit = {
    val dataProcessingStartTime = System.nanoTime()
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
        initBatch(channelId, tuples)
        processInputTuple(Left(inputBatch(currentInputIdx)))
      case EndOfUpstream() =>
        val channel = this.inputGateway.getChannel(channelId)
        val portId = channel.getPortId

        this.inputGateway.getPort(portId).channels(channelId) = true

        if (inputGateway.isPortCompleted(portId)) {
          initBatch(channelId, Array.empty)
          processInputTuple(Right(InputExhausted()))
          outputIterator.appendSpecialTupleToEnd(FinalizePort(portId, input = true))
        }
        if (inputGateway.getAllPorts().forall(portId => inputGateway.isPortCompleted(portId))) {
          // TOOPTIMIZE: assuming all the output ports finalize after all input ports are finalized.
          outputGateway
            .getPortIds()
            .foreach(outputPortId =>
              outputIterator.appendSpecialTupleToEnd(FinalizePort(outputPortId, input = false))
            )

          outputIterator.appendSpecialTupleToEnd(FinalizeOperator())
        }
    }
    dataProcessingTime += (System.nanoTime() - dataProcessingStartTime)
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
      // if this operator is not the final destination of the marker, pass it downstream
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

  private[this] def handleOperatorException(e: Throwable): Unit = {
    asyncRPCClient.send(
      ConsoleMessageTriggered(mkConsoleMessage(actorId, e)),
      CONTROLLER
    )
    logger.warn(e.getLocalizedMessage + "\n" + e.getStackTrace.mkString("\n"))
    // invoke a pause in-place
    asyncRPCServer.execute(PauseWorker(), SELF)
  }

  /**
    * Called by skewed worker in Reshape when it has received the tuples from the helper
    * and is ready to output tuples.
    * The call comes from AcceptMutableStateHandler.
    *
    * @param iterator
    */
  def setCurrentOutputIterator(iterator: Iterator[ITuple]): Unit = {
    outputIterator.setTupleOutput(iterator.map(t => (t, Option.empty)))
  }

}
