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

package org.apache.amber.operator.substringSearch

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.amber.core.executor.OpExecWithClassName
import org.apache.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.amber.operator.filter.FilterOpDesc
import org.apache.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.amber.util.JSONUtils.objectMapper

class SubstringSearchOpDesc extends FilterOpDesc {

  @JsonProperty(required = true)
  @JsonSchemaTitle("attribute")
  @JsonPropertyDescription("column to search substring on")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Substring")
  @JsonPropertyDescription("substring")
  var substring: String = _

  @JsonProperty(required = true, defaultValue = "false")
  @JsonSchemaTitle("Case Sensitive")
  @JsonPropertyDescription("Whether the substring match is case sensitive.")
  var isCaseSensitive: Boolean = false

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
          "org.apache.amber.operator.substringSearch.SubstringSearchOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Substring Search",
      operatorDescription = "Search for Substring(s) in a string column",
      operatorGroupName = OperatorGroupConstants.SEARCH_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )
}
