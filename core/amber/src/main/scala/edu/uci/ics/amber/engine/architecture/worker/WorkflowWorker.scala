package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.Props
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkAck
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.Controller.ReplayStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.logreplay.{ReplayLogGenerator, ReplayOrderEnforcer}
import edu.uci.ics.amber.engine.architecture.messaginglayer.WorkerTimerService
import edu.uci.ics.amber.engine.architecture.scheduling.WorkerConfig
import edu.uci.ics.amber.engine.common.actormessage.{ActorCommand, Backpressure}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  ActorCommandElement,
  DPInputQueueElement,
  FIFOMessageElement,
  TimerBasedControlElement
}
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER

import java.util.concurrent.LinkedBlockingQueue

object WorkflowWorker {
  def props(
      id: ActorVirtualIdentity,
      physicalOp: PhysicalOp,
      workerConf: WorkerConfig
  ): Props =
    Props(
      new WorkflowWorker(
        id,
        physicalOp,
        workerConf
      )
    )

  def getWorkerLogName(id: ActorVirtualIdentity): String = id.name.replace("Worker:", "")

  final case class TriggerSend(msg: WorkflowFIFOMessage)

  sealed trait DPInputQueueElement

  final case class FIFOMessageElement(msg: WorkflowFIFOMessage) extends DPInputQueueElement
  final case class TimerBasedControlElement(control: ControlInvocation) extends DPInputQueueElement
  final case class ActorCommandElement(cmd: ActorCommand) extends DPInputQueueElement
}

class WorkflowWorker(
    workerId: ActorVirtualIdentity,
    physicalOp: PhysicalOp,
    workerConf: WorkerConfig
) extends WorkflowActor(workerConf.logStorageType, workerId) {
  val inputQueue: LinkedBlockingQueue[DPInputQueueElement] =
    new LinkedBlockingQueue()
  var dp = new DataProcessor(
    workerId,
    logManager.sendCommitted
  )
  val timerService = new WorkerTimerService(actorService)

  val dpThread =
    new DPThread(workerId, dp, logManager, inputQueue)

  def setupReplay(): Unit = {
    if (workerConf.replayTo.isDefined) {

      context.parent ! ReplayStatusUpdate(workerId, status = true)

      val (processSteps, messages) = ReplayLogGenerator.generate(logStorage, getLogName)
      val replayTo = workerConf.replayTo.get
      val onReplayComplete = () => {
        logger.info("replay completed!")
        context.parent ! ReplayStatusUpdate(workerId, status = false)
      }
      val orderEnforcer = new ReplayOrderEnforcer(
        logManager,
        processSteps,
        startStep = logManager.getStep,
        replayTo,
        onReplayComplete
      )
      dp.inputGateway.addEnforcer(orderEnforcer)
      messages.foreach(message =>
        dp.inputGateway.getChannel(message.channel).acceptMessage(message)
      )

      logger.info(
        s"setting up replay, " +
          s"current step = ${logManager.getStep} " +
          s"target step = ${workerConf.replayTo.get} " +
          s"# of log record to replay = ${messages.size}"
      )
    }
  }

  override def initState(): Unit = {
    dp.initTimerService(timerService)
    dp.initOperator(
      VirtualIdentityUtils.getWorkerIndex(workerId),
      physicalOp,
      currentOutputIterator = Iterator.empty
    )
    setupReplay()
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
