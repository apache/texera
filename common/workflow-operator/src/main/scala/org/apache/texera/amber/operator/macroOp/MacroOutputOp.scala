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

package org.apache.texera.amber.operator.macroOp

import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, PhysicalPlan, PortIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

// Boundary marker that lives only inside a macro body. Represents external output port
// `portIndex` of the macro: tuples flowing into this marker are emitted out of that
// external port. MacroExpander consumes these markers when splicing the body into the
// parent plan and drops them from the expanded plan.
//
// Ignore `inputPorts` / `outputPorts` on the wire: see MacroInputOp for the
// rationale (operatorInfo derives the marker's port from `portIndex`; the
// PortDescription/PortIdentity mismatch would otherwise break MacroBody parsing).
@JsonIgnoreProperties(Array("inputPorts", "outputPorts"))
class MacroOutputOp extends LogicalOp {

  @JsonProperty(value = "portIndex", required = true)
  @JsonSchemaTitle("Port Index")
  @JsonPropertyDescription("Which external output port (0-based) this marker represents.")
  var portIndex: Int = 0

  @JsonProperty(value = "displayName")
  @JsonSchemaTitle("Display Name")
  var displayName: String = ""

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ) =
    throw new IllegalStateException(
      s"MacroOutputOp(portIndex=$portIndex) must be consumed by MacroExpander before " +
        s"physical-plan compilation. Markers cannot be compiled directly."
    )

  override def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan =
    throw new IllegalStateException(
      s"MacroOutputOp(portIndex=$portIndex) must be consumed by MacroExpander before " +
        s"physical-plan compilation. Markers cannot be compiled directly."
    )

  override def operatorInfo: OperatorInfo = OperatorInfo(
    userFriendlyName = if (displayName.nonEmpty) displayName else s"Output $portIndex",
    operatorDescription =
      "Macro output boundary marker. External output port; consumed by MacroExpander.",
    operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
    inputPorts = List(InputPort(PortIdentity(0))),
    outputPorts = List.empty
  )
}
