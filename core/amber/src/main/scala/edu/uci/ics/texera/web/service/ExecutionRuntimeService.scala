package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.ExecutionStateUpdate
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.BroadcastMessageHandler.BroadcastMessage
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ChannelMarkerHandler.PropagateChannelMarker
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.ControllerInitiateQueryStatistics
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.TakeGlobalCheckpointHandler.TakeGlobalCheckpoint
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.FaultToleranceConfig
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StepHandler.{ContinueProcessing, StopProcessing}
import edu.uci.ics.amber.engine.common.ambermessage.RequireAlignment
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.{ChannelMarkerIdentity, OperatorIdentity}
import edu.uci.ics.texera.web.{SubscriptionManager, WebsocketInput}
import edu.uci.ics.texera.web.model.websocket.request.{SkipTupleRequest, WorkflowInteractionRequest, WorkflowKillRequest, WorkflowPauseRequest, WorkflowResumeRequest, WorkflowStepRequest}
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState._
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ExecutionRuntimeService(
    client: AmberClient,
    stateStore: ExecutionStateStore,
    wsInput: WebsocketInput,
    reconfigurationService: ExecutionReconfigurationService,
    logConf: Option[FaultToleranceConfig],
    physicalPlan: PhysicalPlan
) extends SubscriptionManager
    with LazyLogging {

  //Receive skip tuple
  addSubscription(wsInput.subscribe((req: SkipTupleRequest, uidOpt) => {
    throw new RuntimeException("skipping tuple is temporarily disabled")
  }))

  // Receive execution state update from Amber
  addSubscription(client.registerCallback[ExecutionStateUpdate]((evt: ExecutionStateUpdate) => {
    stateStore.metadataStore.updateState(metadataStore =>
      updateWorkflowState(evt.state, metadataStore)
    )
    if (evt.state == COMPLETED) {
      client.shutdown()
      stateStore.statsStore.updateState(stats => stats.withEndTimeStamp(System.currentTimeMillis()))
    }
  }))

  // Receive Pause
  addSubscription(wsInput.subscribe((req: WorkflowPauseRequest, uidOpt) => {
    stateStore.metadataStore.updateState(metadataStore =>
      updateWorkflowState(PAUSING, metadataStore)
    )
    client.sendAsync(PauseWorkflow())
  }))

  // Receive Resume
  addSubscription(wsInput.subscribe((req: WorkflowResumeRequest, uidOpt) => {
    reconfigurationService.performReconfigurationOnResume()
    stateStore.metadataStore.updateState(metadataStore =>
      updateWorkflowState(RESUMING, metadataStore)
    )
    client.sendAsyncWithCallback[Unit](
      ResumeWorkflow(),
      _ =>
        stateStore.metadataStore.updateState(metadataStore =>
          updateWorkflowState(RUNNING, metadataStore)
        )
    )
  }))

  // Receive Kill
  addSubscription(wsInput.subscribe((req: WorkflowKillRequest, uidOpt) => {
    client.shutdown()
    stateStore.statsStore.updateState(stats => stats.withEndTimeStamp(System.currentTimeMillis()))
    stateStore.metadataStore.updateState(metadataStore =>
      updateWorkflowState(KILLED, metadataStore)
    )
  }))

  // Receive Interaction
  addSubscription(wsInput.subscribe((req: WorkflowInteractionRequest, uidOpt) => {
    assert(
      logConf.nonEmpty,
      "Fault tolerance log folder is not established. Unable to take a global checkpoint."
    )
    val now = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
    val timestamp = now.format(formatter)
    val checkpointId = ChannelMarkerIdentity(s"Interaction_${timestamp}")
    val uri = logConf.get.writeTo.resolve(checkpointId.toString)
    client.sendAsync(TakeGlobalCheckpoint(interactionOnly = false, checkpointId, uri))
  }))

  // Receive Step
  addSubscription(wsInput.subscribe((req: WorkflowStepRequest, uidOpt) => {
    val targetOp = req.targetOp
    val physicalOp = physicalPlan.getPhysicalOpsOfLogicalOp(OperatorIdentity(targetOp)).head.id
    val downstreams = physicalPlan.dag.getDescendants(physicalOp).asScala.toSet ++ Set(physicalOp)
    val downstreamsWithoutTarget = downstreams - physicalOp
    val subDAG = physicalPlan.getSubPlan(downstreams)
    val subDAGWithoutTarget = physicalPlan.getSubPlan(downstreamsWithoutTarget)
    val now = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")
    val timestamp = now.format(formatter)
    req.stepType match{
      case "StepOut" =>
        client.sendAsync(BroadcastMessage(downstreamsWithoutTarget, ContinueProcessing(None)))
        client.sendAsMarker(
          PropagateChannelMarker(
            Set(physicalOp),
            ChannelMarkerIdentity(s"debugging-stepout-${timestamp}"),
            RequireAlignment,
            subDAG,
            downstreamsWithoutTarget,
            StopProcessing()
            ))
      case "StepInto" =>
        client.sendAsync(BroadcastMessage(Iterable(physicalOp), ContinueProcessing(Some(1))))
        client.sendAsync(ControllerInitiateQueryStatistics(None))
      case "StepOver" =>
        client.sendAsync(BroadcastMessage(Iterable(physicalOp), ContinueProcessing(Some(1))))
        client.sendAsync(BroadcastMessage(downstreamsWithoutTarget, ContinueProcessing(None)))
        client.sendAsMarker(
          PropagateChannelMarker(
            Set(physicalOp),
            ChannelMarkerIdentity(s"debugging-stepover-${timestamp}"),
            RequireAlignment,
            subDAG,
            downstreamsWithoutTarget,
            StopProcessing()
          ))
      case other =>
        println(s"Invalid step type: $other")
    }
  }))

}
