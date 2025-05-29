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

package edu.uci.ics.texera.web.model.websocket.request

import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import edu.uci.ics.amber.core.workflow.WorkflowSettings
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.texera.workflow.LogicalLink

case class ReplayExecutionInfo(
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    eid: Long,
    interaction: String
)

case class WorkflowExecuteRequest(
    executionName: String,
    engineVersion: String,
    logicalPlan: LogicalPlanPojo,
    replayFromExecution: Option[ReplayExecutionInfo], // contains execution Id, interaction Id.
    workflowSettings: WorkflowSettings,
    emailNotificationEnabled: Boolean,
    computingUnitId: Int
) extends TexeraWebSocketRequest

case class LogicalPlanPojo(
    operators: List[LogicalOp],
    links: List[LogicalLink],
    opsToViewResult: List[String],
    opsToReuseResult: List[String]
)
