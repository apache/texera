package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{AllForOneStrategy, Props, SupervisorStrategy}
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkAck
import edu.uci.ics.amber.engine.architecture.controller.Controller.{
  ReplayStatusUpdate,
  WorkflowRecoveryStatus
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  FaultToleranceConfig,
  StateRestoreConfig
}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.common.ambermessage.{
  ChannelMarkerPayload,
  ControlPayload,
  WorkflowFIFOMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CLIENT, CONTROLLER, SELF}

import scala.concurrent.duration.DurationInt

object ControllerConfig {
  def default: ControllerConfig =
    ControllerConfig(
      statusUpdateIntervalMs = Option(AmberConfig.getStatusUpdateIntervalInMs),
      stateRestoreConfOpt = None,
      faultToleranceConfOpt = None
    )
}

final case class ControllerConfig(
    statusUpdateIntervalMs: Option[Long],
    stateRestoreConfOpt: Option[StateRestoreConfig],
    faultToleranceConfOpt: Option[FaultToleranceConfig]
)

object Controller {

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

  final case class ReplayStatusUpdate(id: ActorVirtualIdentity, status: Boolean)
  final case class WorkflowRecoveryStatus(isRecovering: Boolean)
}

class Controller(
    val workflow: Workflow,
    val controllerConfig: ControllerConfig
) extends WorkflowActor(
      controllerConfig.faultToleranceConfOpt,
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

  // manages the lifecycle of entire replay process
  // triggers onStart callback when the first worker/controller marks itself as recovering.
  // triggers onComplete callback when all worker/controller finishes recovering.
  private val globalReplayManager = new GlobalReplayManager(
    () => {
      //onStart
      context.parent ! WorkflowRecoveryStatus(true)
    },
    () => {
      //onComplete
      context.parent ! WorkflowRecoveryStatus(false)
    }
  )

  override def initState(): Unit = {
    cp.setupActorService(actorService)
    cp.initWorkflowExecutionController()
    cp.setupTimerService(controllerTimerService)
    cp.setupActorRefService(actorRefMappingService)
    cp.setupLogManager(logManager)
    cp.setupTransferService(transferService)
    val controllerRestoreConf = controllerConfig.stateRestoreConfOpt
    if (controllerRestoreConf.isDefined) {
      globalReplayManager.markRecoveryStatus(CONTROLLER, isRecovering = true)
      setupReplay(
        cp,
        controllerRestoreConf.get,
        () => {
          globalReplayManager.markRecoveryStatus(CONTROLLER, isRecovering = false)
        }
      )
      processMessages()
    }
  }

  override def handleInputMessage(id: Long, workflowMsg: WorkflowFIFOMessage): Unit = {
    val channel = cp.inputGateway.getChannel(workflowMsg.channelId)
    channel.acceptMessage(workflowMsg)
    sender() ! NetworkAck(id, getInMemSize(workflowMsg), getQueuedCredit(workflowMsg.channelId))
    processMessages()
  }

  def processMessages(): Unit = {
    var waitingForInput = false
    while (!waitingForInput) {
      cp.inputGateway.tryPickChannel match {
        case Some(channel) =>
          val msg = channel.take
          val msgToLog = Some(msg).filter(_.payload.isInstanceOf[ControlPayload])
          logManager.withFaultTolerant(msg.channelId, msgToLog) {
            msg.payload match {
              case payload: ControlPayload      => cp.processControlPayload(msg.channelId, payload)
              case marker: ChannelMarkerPayload => // skip marker
              case p                            => throw new RuntimeException(s"controller cannot handle $p")
            }
          }
        case None =>
          waitingForInput = true
      }
    }
  }

  def handleDirectInvocation: Receive = {
    case c: ControlInvocation =>
      // only client and self can send direction invocations
      val source = if (sender() == self) {
        SELF
      } else {
        CLIENT
      }
      val controlChannelId = ChannelIdentity(source, SELF, isControl = true)
      val channel = cp.inputGateway.getChannel(controlChannelId)
      channel.acceptMessage(
        WorkflowFIFOMessage(controlChannelId, channel.getCurrentSeq, c)
      )
      processMessages()
  }

  def handleReplayMessages: Receive = {
    case ReplayStatusUpdate(id, status) =>
      globalReplayManager.markRecoveryStatus(id, status)
  }

  override def receive: Receive = {
    super.receive orElse handleDirectInvocation orElse handleReplayMessages
  }

  /** flow-control */
  override def getQueuedCredit(channelId: ChannelIdentity): Long = {
    0 // no queued credit for controller
  }
  override def handleBackpressure(isBackpressured: Boolean): Unit = {}
  // adopted solution from
  // https://stackoverflow.com/questions/54228901/right-way-of-exception-handling-when-using-akka-actors
  override val supervisorStrategy: SupervisorStrategy =
    AllForOneStrategy(maxNrOfRetries = 0, withinTimeRange = 1.minute) {
      case e: Throwable =>
        val failedWorker = actorRefMappingService.findActorVirtualIdentity(sender())
        logger.error(s"Encountered fatal error from $failedWorker, amber is shutting done.", e)
        cp.asyncRPCServer.execute(FatalError(e, failedWorker), actorId)
        Stop
    }

}
