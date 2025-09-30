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

package edu.uci.ics.amber.operator.limit

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.workflow.PhysicalOp
import edu.uci.ics.amber.operator.{LogicalOp, StateTransferFunc}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort}

import scala.util.{Success, Try}

class LimitOpDesc extends LogicalOp {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Limit")
  @JsonPropertyDescription("the max number of output rows")
  var limit: Int = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.limit.LimitOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withParallelizable(false)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Limit",
      "Limit the number of output rows",
      OperatorGroupConstants.CLEANING_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

  override def runtimeReconfiguration(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      oldLogicalOp: LogicalOp,
      newLogicalOp: LogicalOp
  ): Try[(PhysicalOp, Option[StateTransferFunc])] = {
    val newPhysicalOp = newLogicalOp.getPhysicalOp(workflowId, executionId)
    val stateTransferFunc: StateTransferFunc = (oldOp, newOp) => {
      val oldLimitOp = oldOp.asInstanceOf[LimitOpExec]
      val newLimitOp = newOp.asInstanceOf[LimitOpExec]
      newLimitOp.count = oldLimitOp.count
    }
    Success(newPhysicalOp, Some(stateTransferFunc))
  }
}
