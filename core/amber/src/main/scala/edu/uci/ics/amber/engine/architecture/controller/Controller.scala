package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{AllForOneStrategy, Props, SupervisorStrategy}
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkAck
import edu.uci.ics.amber.engine.architecture.controller.execution.OperatorExecution
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  FaultToleranceConfig,
  StateRestoreConfig
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  ChannelMarkerPayload,
  ControlInvocation
}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.common.model.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelIdentity
import edu.uci.ics.amber.engine.common.{AmberConfig, CheckpointState, SerializedState}
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CLIENT, CONTROLLER, SELF}
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage

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
      workflowContext: WorkflowContext,
      physicalPlan: PhysicalPlan,
      opResultStorage: OpResultStorage,
      controllerConfig: ControllerConfig = ControllerConfig.default
  ): Props =
    Props(
      new Controller(
        workflowContext,
        physicalPlan,
        opResultStorage,
        controllerConfig
      )
    )
}

class Controller(
    workflowContext: WorkflowContext,
    physicalPlan: PhysicalPlan,
    opResultStorage: OpResultStorage,
    controllerConfig: ControllerConfig
) extends WorkflowActor(
      controllerConfig.faultToleranceConfOpt,
      CONTROLLER
    ) {

  actorRefMappingService.registerActorRef(CLIENT, context.parent)
  val controllerTimerService = new ControllerTimerService(controllerConfig, actorService)
  var cp = new ControllerProcessor(
    workflowContext,
    opResultStorage,
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
    attachRuntimeServicesToCPState()
    cp.workflowScheduler.updateSchedule(physicalPlan)
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
        cp.asyncRPCClient.sendToClient(
          FatalError(e, failedWorker)
        ) // only place to actively report fatal error
        Stop
    }

  private def attachRuntimeServicesToCPState(): Unit = {
    cp.setupActorService(actorService)
    cp.setupTimerService(controllerTimerService)
    cp.setupActorRefService(actorRefMappingService)
    cp.setupLogManager(logManager)
    cp.setupTransferService(transferService)
  }

  override def loadFromCheckpoint(chkpt: CheckpointState): Unit = {
    val cpState: ControllerProcessor = chkpt.load(SerializedState.CP_STATE_KEY)
    val outputMessages: Array[WorkflowFIFOMessage] = chkpt.load(SerializedState.OUTPUT_MSG_KEY)
    cp = cpState
    cp.outputHandler = logManager.sendCommitted
    attachRuntimeServicesToCPState()
    // revive all workers.
    cp.workflowExecution.getRunningRegionExecutions.foreach { regionExecution =>
      regionExecution.getAllOperatorExecutions.foreach {
        case (opId, opExecution) =>
          val op = physicalPlan.getOperator(opId)
          op.build(
            actorService,
            OperatorExecution(), //use dummy value here
            regionExecution.region.resourceConfig.get.operatorConfigs(opId),
            controllerConfig.stateRestoreConfOpt,
            controllerConfig.faultToleranceConfOpt
          )
      }
    }
    outputMessages.foreach(transferService.send)
    cp.asyncRPCClient.sendToClient(
      ExecutionStatsUpdate(
        cp.workflowExecution.getAllRegionExecutionsStats
      )
    )
    globalReplayManager.markRecoveryStatus(CONTROLLER, isRecovering = false)
  }
}
