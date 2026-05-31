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

package org.apache.texera.web.model.websocket.request

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.texera.amber.core.workflow.{PhysicalPlan, WorkflowSettings}

case class ReplayExecutionInfo(
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    eid: Long,
    interaction: String
)

/**
  * Execution request the client sends to the ComputingUnitMaster. The client (frontend / agent
  * service) compiles the workflow against the workflow-compiling-service and ships the resulting
  * ready-to-run [[PhysicalPlan]] here, so the CU neither compiles nor authenticates — it just runs
  * the plan. `opsToViewResult` (logical operator ids) is used to mark which output ports need
  * result storage.
  */
case class WorkflowExecuteRequest(
    executionName: String,
    engineVersion: String,
    physicalPlan: PhysicalPlan,
    opsToViewResult: List[String] = List.empty,
    replayFromExecution: Option[ReplayExecutionInfo], // contains execution Id, interaction Id.
    workflowSettings: WorkflowSettings,
    emailNotificationEnabled: Boolean,
    computingUnitId: Int
) extends TexeraWebSocketRequest
