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

package edu.uci.ics.amber.operator.regex

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.workflow.PhysicalOp
import edu.uci.ics.amber.operator.filter.FilterOpDesc
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort}

class RegexOpDesc extends FilterOpDesc {

  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("column to search regex on")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(value = "regex", required = true)
  @JsonPropertyDescription("regular expression")
  var regex: String = _

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("Case Insensitive")
  @JsonPropertyDescription("regex match is case sensitive")
  var caseInsensitive: Boolean = _

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
          "edu.uci.ics.amber.operator.regex.RegexOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Regular Expression",
      operatorDescription = "Search a regular expression in a string column",
      operatorGroupName = OperatorGroupConstants.SEARCH_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )
}
