package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.ExecutionStateUpdate
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  EmptyRequest,
  TakeGlobalCheckpointRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState._
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.FaultToleranceConfig
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.virtualidentity.ChannelMarkerIdentity
import edu.uci.ics.texera.web.model.websocket.request._
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.texera.web.{SubscriptionManager, WebsocketInput}

import java.util.UUID

class ExecutionRuntimeService(
    client: AmberClient,
    stateStore: ExecutionStateStore,
    wsInput: WebsocketInput,
    reconfigurationService: ExecutionReconfigurationService,
    logConf: Option[FaultToleranceConfig]
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
  addSubscription(wsInput.subscribe((req: WorkflowCheckpointRequest, uidOpt) => {
    assert(
      logConf.nonEmpty,
      "Fault tolerance log folder is not established. Unable to take a global checkpoint."
    )
    val checkpointId = ChannelMarkerIdentity(s"Checkpoint_${UUID.randomUUID().toString}")
    val uri = logConf.get.writeTo.resolve(checkpointId.toString)
    client.controllerInterface.takeGlobalCheckpoint(
      TakeGlobalCheckpointRequest(estimationOnly = false, checkpointId, uri.toString),
      ()
    )
  }))

}
