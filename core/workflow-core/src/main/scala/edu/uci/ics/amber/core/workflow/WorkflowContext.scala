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

package edu.uci.ics.amber.core.workflow

import edu.uci.ics.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID,
  DEFAULT_WORKFLOW_SETTINGS
}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}

object WorkflowContext {
  val DEFAULT_EXECUTION_ID: ExecutionIdentity = ExecutionIdentity(1L)
  val DEFAULT_WORKFLOW_ID: WorkflowIdentity = WorkflowIdentity(1L)
  val DEFAULT_WORKFLOW_SETTINGS: WorkflowSettings = WorkflowSettings(
    dataTransferBatchSize = 400 // TODO: make this configurable
  )
}
class WorkflowContext(
    var workflowId: WorkflowIdentity = DEFAULT_WORKFLOW_ID,
    var executionId: ExecutionIdentity = DEFAULT_EXECUTION_ID,
    var workflowSettings: WorkflowSettings = DEFAULT_WORKFLOW_SETTINGS
)
