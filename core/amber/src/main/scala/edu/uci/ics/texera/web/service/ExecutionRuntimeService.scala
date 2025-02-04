package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.ExecutionStateUpdate
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{BroadcastMessageRequest, ContinueProcessingRequest, EmptyRequest, PropagateChannelMarkerRequest, QueryStatisticsRequest, StopProcessingRequest, TakeGlobalCheckpointRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState._
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.FaultToleranceConfig
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.core.virtualidentity.{ChannelMarkerIdentity, OperatorIdentity}
import edu.uci.ics.amber.core.workflow.PhysicalPlan
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ChannelMarkerType.REQUIRE_ALIGNMENT
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_STOP_PROCESSING
import edu.uci.ics.texera.web.{SubscriptionManager, WebsocketInput}
import edu.uci.ics.texera.web.model.websocket.request.{SkipTupleRequest, WorkflowInteractionRequest, WorkflowKillRequest, WorkflowPauseRequest, WorkflowResumeRequest, WorkflowStepRequest}
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ExecutionRuntimeService(
    client: AmberClient,
    stateStore: ExecutionStateStore,
    wsInput: WebsocketInput,
    reconfigurationService: ExecutionReconfigurationService,
    logConf: Option[FaultToleranceConfig],
    physicalPlan: PhysicalPlan,
    isReplaying:Boolean
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
    client.controllerInterface.pauseWorkflow(EmptyRequest(), ())
  }))

  // Receive Resume
  addSubscription(wsInput.subscribe((req: WorkflowResumeRequest, uidOpt) => {
    reconfigurationService.performReconfigurationOnResume()
    stateStore.metadataStore.updateState(metadataStore =>
      updateWorkflowState(RESUMING, metadataStore)
    )
    client.controllerInterface
      .resumeWorkflow(EmptyRequest(), ())
      .onSuccess(_ =>
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
    if (!isReplaying) {
      val now = System.currentTimeMillis()
      val start = stateStore.statsStore.getState.startTimeStamp
      // Calculate the difference in milliseconds
      val diffMs = now - start

      // Convert to total seconds
      val totalSeconds = diffMs / 1000

      // Extract minutes and seconds
      val minutes = totalSeconds / 60
      val seconds = totalSeconds % 60

      // Produce a string like "3:07"
      val diffString = f"$minutes mins $seconds%02d secs"

      val checkpointId = ChannelMarkerIdentity(s"Inspection at ${diffString}".replace(" ", "_"))
      val uri = logConf.get.writeTo.resolve(checkpointId.toString)
      client.controllerInterface.takeGlobalCheckpoint(
        TakeGlobalCheckpointRequest(estimationOnly = false, checkpointId, uri.toString),
        ()
      )
    }
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
        client.controllerInterface.broadcastMessage(BroadcastMessageRequest(downstreamsWithoutTarget.toSeq, ContinueProcessingRequest(-1)), ())
        Thread.sleep(1000)
        client.controllerInterface.propagateChannelMarker(
          PropagateChannelMarkerRequest(
            Seq(physicalOp),
            ChannelMarkerIdentity(s"debugging-stepout-${timestamp}"),
            REQUIRE_ALIGNMENT,
            subDAG.operators.map(_.id).toSeq,
            downstreams.toSeq,
            StopProcessingRequest(),
            METHOD_STOP_PROCESSING.getBareMethodName
            ), ()).onSuccess(ret =>
          client.controllerInterface.controllerInitiateQueryStatistics(QueryStatisticsRequest(Seq.empty), ())
        )
      case "StepInto" =>
        client.controllerInterface.broadcastMessage(BroadcastMessageRequest(Seq(physicalOp), ContinueProcessingRequest(1)), ()).onSuccess(ret =>
          client.controllerInterface.controllerInitiateQueryStatistics(QueryStatisticsRequest(Seq.empty), ())
        )
      case "StepOver" =>
        client.controllerInterface.broadcastMessage(BroadcastMessageRequest(Seq(physicalOp), ContinueProcessingRequest(1)), ())
        client.controllerInterface.broadcastMessage(BroadcastMessageRequest(downstreamsWithoutTarget.toSeq, ContinueProcessingRequest(-1)), ())
        Thread.sleep(1000)
        client.controllerInterface.propagateChannelMarker(
          PropagateChannelMarkerRequest(
            Seq(physicalOp),
            ChannelMarkerIdentity(s"debugging-stepover-${timestamp}"),
            REQUIRE_ALIGNMENT,
            subDAG.operators.map(_.id).toSeq,
            downstreams.toSeq,
            StopProcessingRequest(),
            METHOD_STOP_PROCESSING.getBareMethodName
          ), ()).onSuccess(ret =>
          client.controllerInterface.controllerInitiateQueryStatistics(QueryStatisticsRequest(Seq.empty), ())
        )
      case other =>
        println(s"Invalid step type: $other")
    }
  }))

}
