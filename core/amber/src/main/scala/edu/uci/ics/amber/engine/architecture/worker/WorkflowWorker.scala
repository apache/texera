package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.Props
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkAck
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.messaginglayer.WorkerTimerService
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.TriggerSend
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.BackpressureHandler.Backpressure
import edu.uci.ics.amber.engine.common.ambermessage._
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER

import java.util.concurrent.LinkedBlockingQueue

object WorkflowWorker {
  def props(
      id: ActorVirtualIdentity,
      workerIndex: Int,
      workerLayer: OpExecConfig
  ): Props =
    Props(
      new WorkflowWorker(
        id,
        workerIndex: Int,
        workerLayer: OpExecConfig
      )
    )

  def getWorkerLogName(id: ActorVirtualIdentity): String = id.name.replace("Worker:", "")

  final case class TriggerSend(msg: WorkflowFIFOMessage)
}

class WorkflowWorker(
    actorId: ActorVirtualIdentity,
    workerIndex: Int,
    workerLayer: OpExecConfig
) extends WorkflowActor(actorId) {
  val inputQueue: LinkedBlockingQueue[Either[WorkflowFIFOMessage, ControlInvocation]] =
    new LinkedBlockingQueue()
  var dp = new DataProcessor(
    actorId,
    workerIndex,
    workerLayer.initIOperatorExecutor((workerIndex, workerLayer)),
    workerLayer,
    sendMessageFromDPToMain
  )
  val timerService = new WorkerTimerService(actorService)
  val dpThread = new DPThread(actorId, dp, inputQueue)

  def sendMessageFromDPToMain(msg: WorkflowFIFOMessage): Unit = {
    // limitation: TriggerSend will be processed after input messages before it.
    self ! TriggerSend(msg)
  }

  def handleSendFromDP: Receive = {
    case TriggerSend(msg) =>
      transferService.send(msg)
  }

  def handleDirectInvocation: Receive = {
    case c: ControlInvocation =>
      inputQueue.put(Right(c))
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    super.preRestart(reason, message)
    logger.error(s"Encountered fatal error, worker is shutting done.", reason)
    postStop()
    dp.asyncRPCClient.send(
      FatalError(reason, Some(actorId)),
      CONTROLLER
    )
  }

  override def receive: Receive = {
    super.receive orElse handleSendFromDP orElse handleDirectInvocation
  }

  override def handleInputMessage(id: Long, workflowMsg: WorkflowFIFOMessage): Unit = {
    inputQueue.put(Left(workflowMsg))
    sender ! NetworkAck(id)
  }

  /** flow-control */
  override def getSenderCredits(channelID: ChannelID): Int =
    dp.getSenderCredits(channelID)

  override def initState(): Unit = {
    dp.InitTimerService(timerService)
    dpThread.start()
  }

  override def postStop(): Unit = {
    super.postStop()
    timerService.stopAdaptiveBatching()
    dpThread.stop()
  }

  override def handleBackpressure(isBackpressured: Boolean): Unit = {
    val backpressureMessage = ControlInvocation(0, Backpressure(isBackpressured))
    inputQueue.put(Right(backpressureMessage))
  }
}
