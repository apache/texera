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

package org.apache.texera.amber.engine.architecture.coordinator

import org.apache.pekko.actor.SupervisorStrategy.Stop
import org.apache.pekko.actor.{AllForOneStrategy, Props, SupervisorStrategy}
import org.apache.texera.web.model.websocket.response.RegionUpdateEvent
import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.amber.core.virtualidentity.ChannelIdentity
import org.apache.texera.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import org.apache.texera.amber.engine.architecture.common.WorkflowActor.NetworkAck
import org.apache.texera.amber.engine.architecture.common.{ExecutorDeployment, WorkflowActor}
import org.apache.texera.amber.engine.architecture.coordinator.execution.OperatorExecution
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  ControlInvocation,
  EmbeddedControlMessage
}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  FaultToleranceConfig,
  StateRestoreConfig
}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import org.apache.texera.amber.engine.common.ambermessage.{
  DirectControlMessagePayload,
  WorkflowFIFOMessage
}
import org.apache.texera.amber.engine.common.virtualidentity.util.{CLIENT, COORDINATOR, SELF}
import org.apache.texera.amber.engine.common.{CheckpointState, SerializedState}
import org.apache.texera.web.SessionState

import scala.concurrent.duration.DurationInt

object CoordinatorConfig {
  def default: CoordinatorConfig =
    CoordinatorConfig(
      statusUpdateIntervalMs = Option(ApplicationConfig.getStatusUpdateIntervalInMs),
      runtimeStatisticsPersistenceIntervalMs =
        Option(ApplicationConfig.getRuntimeStatisticsPersistenceIntervalInMs),
      stateRestoreConfOpt = None,
      faultToleranceConfOpt = None
    )
}

final case class CoordinatorConfig(
    statusUpdateIntervalMs: Option[Long],
    runtimeStatisticsPersistenceIntervalMs: Option[Long],
    stateRestoreConfOpt: Option[StateRestoreConfig],
    faultToleranceConfOpt: Option[FaultToleranceConfig]
)

object Coordinator {

  def props(
      workflowContext: WorkflowContext,
      physicalPlan: PhysicalPlan,
      coordinatorConfig: CoordinatorConfig = CoordinatorConfig.default
  ): Props =
    Props(
      new Coordinator(
        workflowContext,
        physicalPlan,
        coordinatorConfig
      )
    )
}

class Coordinator(
    workflowContext: WorkflowContext,
    physicalPlan: PhysicalPlan,
    coordinatorConfig: CoordinatorConfig
) extends WorkflowActor(
      coordinatorConfig.faultToleranceConfOpt,
      COORDINATOR
    ) {

  actorRefMappingService.registerActorRef(CLIENT, context.parent)
  val coordinatorTimerService = new CoordinatorTimerService(coordinatorConfig, actorService)
  var cp = new CoordinatorProcessor(
    workflowContext,
    coordinatorConfig,
    actorId,
    logManager.sendCommitted
  )

  // manages the lifecycle of entire replay process
  // triggers onStart callback when the first worker/coordinator marks itself as recovering.
  // triggers onComplete callback when all worker/coordinator finishes recovering.
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
    cp.workflowExecutionManager.schedule = cp.workflowScheduler.getSchedule

    val regions: List[(Long, List[String])] =
      cp.workflowScheduler.getSchedule.getRegions.map { region =>
        (region.id.id, region.physicalOps.map(_.id.logicalOpId.id).toList)
      }

    SessionState.getAllSessionStates.foreach { state =>
      state.send(RegionUpdateEvent(regions))
    }

    val coordinatorRestoreConf = coordinatorConfig.stateRestoreConfOpt
    if (coordinatorRestoreConf.isDefined) {
      globalReplayManager.markRecoveryStatus(COORDINATOR, isRecovering = true)
      setupReplay(
        cp,
        coordinatorRestoreConf.get,
        () => {
          globalReplayManager.markRecoveryStatus(COORDINATOR, isRecovering = false)
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
          val msgToLog = Some(msg).filter(_.payload.isInstanceOf[DirectControlMessagePayload])
          logManager.withFaultTolerant(msg.channelId, msgToLog) {
            msg.payload match {
              case payload: DirectControlMessagePayload => cp.processDCM(msg.channelId, payload)
              case _: EmbeddedControlMessage            => // skip ECM
              case p                                    => throw new RuntimeException(s"coordinator cannot handle $p")
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
    0 // no queued credit for coordinator
  }

  override def handleBackpressure(isBackpressured: Boolean): Unit = {}

  // Use AllForOneStrategy to stop all children on any fatal error and report it to the client.
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
    cp.setupTimerService(coordinatorTimerService)
    cp.setupActorRefService(actorRefMappingService)
    cp.setupLogManager(logManager)
    cp.setupTransferService(transferService)
  }

  override def loadFromCheckpoint(chkpt: CheckpointState): Unit = {
    val cpState: CoordinatorProcessor = chkpt.load(SerializedState.CP_STATE_KEY)
    val outputMessages: Array[WorkflowFIFOMessage] = chkpt.load(SerializedState.OUTPUT_MSG_KEY)
    cp = cpState
    cp.outputHandler = logManager.sendCommitted
    attachRuntimeServicesToCPState()
    // revive all workers.
    cp.workflowExecution.getRunningRegionExecutions.foreach { regionExecution =>
      regionExecution.getAllOperatorExecutions.foreach {
        case (opId, opExecution) =>
          val op = physicalPlan.getOperator(opId)
          ExecutorDeployment.createWorkers(
            op,
            actorService,
            OperatorExecution(), //use dummy value here
            regionExecution.region.resourceConfig.get.operatorConfigs(opId),
            coordinatorConfig.stateRestoreConfOpt,
            coordinatorConfig.faultToleranceConfOpt
          )
      }
    }
    outputMessages.foreach(transferService.send)
    cp.asyncRPCClient.sendToClient(
      ExecutionStatsUpdate(
        cp.workflowExecution.getAllRegionExecutionsStats
      )
    )
    globalReplayManager.markRecoveryStatus(COORDINATOR, isRecovering = false)
  }
}
