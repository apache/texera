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

package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.engine.architecture.common.{
  AkkaActorRefMappingService,
  AkkaActorService,
  AkkaMessageTransferService,
  AmberProcessor
}
import edu.uci.ics.amber.engine.architecture.controller.execution.WorkflowExecution
import edu.uci.ics.amber.engine.architecture.logreplay.ReplayLogManager
import edu.uci.ics.amber.engine.architecture.scheduling.WorkflowExecutionCoordinator
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

class ControllerProcessor(
    workflowContext: WorkflowContext,
    controllerConfig: ControllerConfig,
    actorId: ActorVirtualIdentity,
    outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit
) extends AmberProcessor(actorId, outputHandler) {

  val workflowExecution: WorkflowExecution = WorkflowExecution()
  val workflowScheduler: WorkflowScheduler =
    new WorkflowScheduler(workflowContext, actorId)
  val workflowExecutionCoordinator: WorkflowExecutionCoordinator = new WorkflowExecutionCoordinator(
    () => this.workflowScheduler.getNextRegions,
    workflowExecution,
    controllerConfig,
    asyncRPCClient
  )

  private val initializer = new ControllerAsyncRPCHandlerInitializer(this)

  @transient var controllerTimerService: ControllerTimerService = _

  def setupTimerService(controllerTimerService: ControllerTimerService): Unit = {
    this.controllerTimerService = controllerTimerService
  }

  @transient var transferService: AkkaMessageTransferService = _

  def setupTransferService(transferService: AkkaMessageTransferService): Unit = {
    this.transferService = transferService
  }

  @transient var actorService: AkkaActorService = _

  def setupActorService(akkaActorService: AkkaActorService): Unit = {
    this.actorService = akkaActorService
  }

  @transient var actorRefService: AkkaActorRefMappingService = _

  def setupActorRefService(actorRefService: AkkaActorRefMappingService): Unit = {
    this.actorRefService = actorRefService
  }

  @transient var logManager: ReplayLogManager = _

  def setupLogManager(logManager: ReplayLogManager): Unit = {
    this.logManager = logManager
  }

}
