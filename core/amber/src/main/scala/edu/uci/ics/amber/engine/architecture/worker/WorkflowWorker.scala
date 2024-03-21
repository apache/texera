package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.Props
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkAck
import edu.uci.ics.amber.engine.architecture.controller.Controller.ReplayStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.messaginglayer.WorkerTimerService
import edu.uci.ics.amber.engine.architecture.scheduling.config.WorkerConfig
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker._
import edu.uci.ics.amber.engine.common.{CheckpointState, SerializedState}
import edu.uci.ics.amber.engine.common.actormessage.{ActorCommand, Backpressure}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{ChannelIdentity, ChannelMarkerIdentity}

import java.net.URI
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable

object WorkflowWorker {
  def props(
      workerConfig: WorkerConfig,
      replayInitialization: WorkerReplayInitialization
  ): Props =
    Props(
      new WorkflowWorker(
        workerConfig,
        replayInitialization
      )
    )

  final case class TriggerSend(msg: WorkflowFIFOMessage)

  final case class MainThreadDelegateMessage(closure: WorkflowWorker => Unit)

  sealed trait DPInputQueueElement

  final case class FIFOMessageElement(msg: WorkflowFIFOMessage) extends DPInputQueueElement
  final case class TimerBasedControlElement(control: ControlInvocation) extends DPInputQueueElement
  final case class ActorCommandElement(cmd: ActorCommand) extends DPInputQueueElement

  final case class WorkerReplayInitialization(
      restoreConfOpt: Option[StateRestoreConfig] = None,
      faultToleranceConfOpt: Option[FaultToleranceConfig] = None
  )
  final case class StateRestoreConfig(readFrom: URI, replayDestination: ChannelMarkerIdentity)

  final case class FaultToleranceConfig(writeTo: URI)
}

class WorkflowWorker(
    workerConfig: WorkerConfig,
    replayInitialization: WorkerReplayInitialization
) extends WorkflowActor(replayInitialization.faultToleranceConfOpt, workerConfig.workerId) {
  val inputQueue: LinkedBlockingQueue[DPInputQueueElement] =
    new LinkedBlockingQueue()
  var dp = new DataProcessor(workerConfig.workerId, logManager.sendCommitted)
  val timerService = new WorkerTimerService(actorService)

  var dpThread: DPThread = _

  val recordedInputs =
    new mutable.HashMap[ChannelMarkerIdentity, mutable.ArrayBuffer[WorkflowFIFOMessage]]()

  override def initState(): Unit = {
    dp.initTimerService(timerService)
    if (replayInitialization.restoreConfOpt.isDefined) {
      context.parent ! ReplayStatusUpdate(actorId, status = true)
      setupReplay(
        dp,
        replayInitialization.restoreConfOpt.get,
        () => {
          logger.info("replay completed!")
          context.parent ! ReplayStatusUpdate(actorId, status = false)
        }
      )
    }
    // dp is ready
    dpThread = new DPThread(workerConfig.workerId, dp, logManager, inputQueue)
    dpThread.start()
  }

  def handleDirectInvocation: Receive = {
    case c: ControlInvocation =>
      inputQueue.put(TimerBasedControlElement(c))
  }

  def handleTriggerClosure: Receive = {
    case t: MainThreadDelegateMessage =>
      t.closure(this)
  }

  def handleActorCommand: Receive = {
    case c: ActorCommand =>
      println(c)
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    logger.error(s"Encountered fatal error, worker is shutting done.", reason)
    postStop()
    dp.asyncRPCClient.send(
      FatalError(reason, Some(workerConfig.workerId)),
      CONTROLLER
    )
  }

  override def receive: Receive = {
    super.receive orElse handleDirectInvocation orElse handleTriggerClosure
  }

  override def handleInputMessage(id: Long, workflowMsg: WorkflowFIFOMessage): Unit = {
    inputQueue.put(FIFOMessageElement(workflowMsg))
    recordedInputs.values.foreach(_.append(workflowMsg))
    sender() ! NetworkAck(id, getInMemSize(workflowMsg), getQueuedCredit(workflowMsg.channelId))
  }

  /** flow-control */
  override def getQueuedCredit(channelId: ChannelIdentity): Long = {
    dp.getQueuedCredit(channelId)
  }

  override def postStop(): Unit = {
    super.postStop()
    timerService.stopAdaptiveBatching()
    dpThread.stop()
    logManager.terminate()
  }

  override def handleBackpressure(isBackpressured: Boolean): Unit = {
    inputQueue.put(ActorCommandElement(Backpressure(isBackpressured)))
  }

  override def loadFromCheckpoint(chkpt: CheckpointState): Unit = {
    logger.info("start loading from checkpoint.")
    val inflightMessages: mutable.ArrayBuffer[WorkflowFIFOMessage] =
      chkpt.load(SerializedState.IN_FLIGHT_MSG_KEY)
    logger.info("inflight messages restored.")
    val dpState: DataProcessor = chkpt.load(SerializedState.DP_STATE_KEY)
    logger.info("dp state restored")
    val queuedMessages: mutable.ArrayBuffer[WorkflowFIFOMessage] =
      chkpt.load(SerializedState.DP_QUEUED_MSG_KEY)
    logger.info("queued messages restored.")
    val outputMessages: Array[WorkflowFIFOMessage] = chkpt.load(SerializedState.OUTPUT_MSG_KEY)
    logger.info("output messages restored.")
    dp = dpState // overwrite dp state
    dp.outputHandler = logManager.sendCommitted
    dp.initTimerService(timerService)
    logger.info("start re-initialize executor from checkpoint.")
    val (executor, iter) = dp.serializationManager.restoreExecutorState(chkpt)
    dp.executor = executor
    logger.info("re-initialize executor done.")
    dp.outputManager.outputIterator.setTupleOutput(iter)
    logger.info("set tuple output done.")
    queuedMessages.foreach(msg => inputQueue.put(FIFOMessageElement(msg)))
    inflightMessages.foreach(msg => inputQueue.put(FIFOMessageElement(msg)))
    outputMessages.foreach(transferService.send)
    logger.info("restored all messages done.")
    context.parent ! ReplayStatusUpdate(actorId, status = false)
  }
}
