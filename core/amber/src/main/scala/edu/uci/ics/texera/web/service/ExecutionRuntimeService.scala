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
import edu.uci.ics.amber.core.virtualidentity.ChannelMarkerIdentity
import edu.uci.ics.texera.web.model.websocket.request._
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.storage.ExecutionStateStore.updateWorkflowState
import edu.uci.ics.texera.web.{SubscriptionManager, WebsocketInput}

import java.net.URI
import java.util.UUID

class ExecutionRuntimeService(
    client: AmberClient,
    stateStore: ExecutionStateStore,
    wsInput: WebsocketInput,
    reconfigurationService: ExecutionReconfigurationService,
    logConf: Option[FaultToleranceConfig],
    workflowId: Long,
    emailNotificationEnabled: Boolean,
    userEmailOpt: Option[String],
    sessionUri: URI
) extends SubscriptionManager
    with LazyLogging {

  private val emailNotificationService = for {
    email <- userEmailOpt
    if emailNotificationEnabled
  } yield new EmailNotificationService(
    new WorkflowEmailNotifier(
      workflowId,
      email,
      sessionUri
    )
  )

  //Receive skip tuple
  addSubscription(wsInput.subscribe((req: SkipTupleRequest, uidOpt) => {
    throw new RuntimeException("skipping tuple is temporarily disabled")
  }))

  // Receive execution state update from Amber
  addSubscription(client.registerCallback[ExecutionStateUpdate]((evt: ExecutionStateUpdate) => {
    stateStore.metadataStore.updateState(metadataStore =>
      updateWorkflowState(evt.state, metadataStore)
    )

    emailNotificationService.foreach(_.processEmailNotificationIfNeeded(evt.state))

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

  override def unsubscribeAll(): Unit = {
    super.unsubscribeAll()
    emailNotificationService.foreach(_.shutdown())
  }

}
