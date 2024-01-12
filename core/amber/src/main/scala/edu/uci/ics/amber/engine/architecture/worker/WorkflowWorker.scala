package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.Props
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkAck
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.Controller.ReplayStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.messaginglayer.WorkerTimerService
import edu.uci.ics.amber.engine.architecture.scheduling.WorkerConfig
import edu.uci.ics.amber.engine.common.actormessage.{ActorCommand, Backpressure}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  ActorCommandElement,
  DPInputQueueElement,
  FIFOMessageElement,
  TimerBasedControlElement,
  WorkerReplayInitialization
}
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER

import java.net.URI
import java.util.concurrent.LinkedBlockingQueue

object WorkflowWorker {
  def props(
      id: ActorVirtualIdentity,
      physicalOp: PhysicalOp,
      workerConf: WorkerConfig,
      replayInitialization: WorkerReplayInitialization
  ): Props =
    Props(
      new WorkflowWorker(
        id,
        physicalOp,
        workerConf,
        replayInitialization
      )
    )

  def getWorkerLogName(id: ActorVirtualIdentity): String = id.name.replace("Worker:", "")

  final case class TriggerSend(msg: WorkflowFIFOMessage)

  sealed trait DPInputQueueElement

  final case class FIFOMessageElement(msg: WorkflowFIFOMessage) extends DPInputQueueElement
  final case class TimerBasedControlElement(control: ControlInvocation) extends DPInputQueueElement
  final case class ActorCommandElement(cmd: ActorCommand) extends DPInputQueueElement

  final case class WorkerReplayInitialization(
      restoreConfOpt: Option[WorkerStateRestoreConfig] = None,
      replayLogConfOpt: Option[WorkerReplayLoggingConfig] = None
  )
  final case class WorkerStateRestoreConfig(readFrom: URI, replayTo: Long)

  final case class WorkerReplayLoggingConfig(writeTo: URI)
}

class WorkflowWorker(
    workerId: ActorVirtualIdentity,
    physicalOp: PhysicalOp,
    workerConf: WorkerConfig,
    replayInitialization: WorkerReplayInitialization
) extends WorkflowActor(replayInitialization.replayLogConfOpt, workerId) {
  val inputQueue: LinkedBlockingQueue[DPInputQueueElement] =
    new LinkedBlockingQueue()
  var dp = new DataProcessor(
    workerId,
    logManager.sendCommitted
  )
  val timerService = new WorkerTimerService(actorService)

  val dpThread =
    new DPThread(workerId, dp, logManager, inputQueue)

  override def initState(): Unit = {
    dp.initTimerService(timerService)
    dp.initOperator(
      VirtualIdentityUtils.getWorkerIndex(workerId),
      physicalOp,
      currentOutputIterator = Iterator.empty
    )
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
    dpThread.start()
  }

  def handleDirectInvocation: Receive = {
    case c: ControlInvocation =>
      inputQueue.put(TimerBasedControlElement(c))
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    logger.error(s"Encountered fatal error, worker is shutting done.", reason)
    postStop()
    dp.asyncRPCClient.send(
      FatalError(reason, Some(workerId)),
      CONTROLLER
    )
  }

  override def receive: Receive = {
    super.receive orElse handleDirectInvocation
  }

  override def handleInputMessage(id: Long, workflowMsg: WorkflowFIFOMessage): Unit = {
    inputQueue.put(FIFOMessageElement(workflowMsg))
    sender ! NetworkAck(id, getInMemSize(workflowMsg), getQueuedCredit(workflowMsg.channel))
  }

  /** flow-control */
  override def getQueuedCredit(channelID: ChannelID): Long = {
    dp.getQueuedCredit(channelID)
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
}
