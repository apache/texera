package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{AllForOneStrategy, Props, SupervisorStrategy}
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkAck
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, ControlPayload, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CLIENT, CONTROLLER, SELF}

import scala.concurrent.duration.DurationInt

object ControllerConfig {
  def default: ControllerConfig =
    ControllerConfig(
      monitoringIntervalMs = Option(AmberConfig.monitoringIntervalInMs),
      skewDetectionIntervalMs = Option(AmberConfig.reshapeSkewDetectionIntervalInMs),
      statusUpdateIntervalMs =
        Option(AmberConfig.getStatusUpdateIntervalInMs),
      AmberConfig.faultToleranceLogStorage
    )
}

final case class ControllerConfig(
    monitoringIntervalMs: Option[Long],
    skewDetectionIntervalMs: Option[Long],
    statusUpdateIntervalMs: Option[Long],
    logStorageType: String
)

object Controller {

  val recoveryDelay: Long = AmberConfig.delayBeforeRecovery

  def props(
      workflow: Workflow,
      controllerConfig: ControllerConfig = ControllerConfig.default
  ): Props =
    Props(
      new Controller(
        workflow,
        controllerConfig
      )
    )
}

class Controller(
    val workflow: Workflow,
    val controllerConfig: ControllerConfig
) extends WorkflowActor(
      controllerConfig.logStorageType,
      CONTROLLER
    ) {

  actorRefMappingService.registerActorRef(CLIENT, context.parent)
  val controllerTimerService = new ControllerTimerService(controllerConfig, actorService)
  val cp = new ControllerProcessor(
    workflow,
    controllerConfig,
    actorId,
    logManager.sendCommitted
  )

  override def initState(): Unit = {
    cp.setupActorService(actorService)
    cp.setupTimerService(controllerTimerService)
    cp.setupActorRefService(actorRefMappingService)
  }

  override def handleInputMessage(id: Long, workflowMsg: WorkflowFIFOMessage): Unit = {
    val channel = cp.inputGateway.getChannel(workflowMsg.channel)
    channel.acceptMessage(workflowMsg)
    sender ! NetworkAck(id, getInMemSize(workflowMsg), getQueuedCredit(workflowMsg.channel))
    processMessages()
  }

  def processMessages(): Unit = {
    var waitingForInput = false
    while (!waitingForInput) {
      cp.inputGateway.tryPickChannel match {
        case Some(channel) =>
          val msg = channel.take
          logManager.withFaultTolerant(msg.channel, Some(msg)) {
            msg.payload match {
              case payload: ControlPayload => cp.processControlPayload(msg.channel, payload)
              case p                       => throw new RuntimeException(s"controller cannot handle $p")
            }
          }
        case None => waitingForInput = true
      }
    }
  }

  def handleDirectInvocation: Receive = {
    case c: ControlInvocation =>
      // only client and self can send direction invocations
      val source = if (sender == self) {
        SELF
      } else {
        CLIENT
      }
      val controlChannelId = ChannelID(source, SELF, isControl = true)
      val channel = cp.inputGateway.getChannel(controlChannelId)
      channel.acceptMessage(
        WorkflowFIFOMessage(controlChannelId, channel.getCurrentSeq, c)
      )
      processMessages()
  }

  override def receive: Receive = {
    super.receive orElse handleDirectInvocation
  }

  /** flow-control */
  override def getQueuedCredit(channelID: ChannelID): Long = {
    0 // no queued credit for controller
  }
  override def handleBackpressure(isBackpressured: Boolean): Unit = {}
  // adopted solution from
  // https://stackoverflow.com/questions/54228901/right-way-of-exception-handling-when-using-akka-actors
  override val supervisorStrategy: SupervisorStrategy =
    AllForOneStrategy(maxNrOfRetries = 0, withinTimeRange = 1.minute) {
      case e: Throwable =>
        val failedWorker = actorRefMappingService.findActorVirtualIdentity(sender)
        logger.error(s"Encountered fatal error from $failedWorker, amber is shutting done.", e)
        cp.asyncRPCServer.execute(FatalError(e, failedWorker), actorId)
        Stop
    }

}
