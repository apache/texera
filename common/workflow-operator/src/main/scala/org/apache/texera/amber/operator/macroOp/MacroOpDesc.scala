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
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalPlan, PortIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

// A macro instance on the parent canvas. Carries identity + link mode + (optionally)
// an embedded body. MacroOpDesc never reaches physical-plan compilation: MacroExpander
// (in workflow-compiling-service) consumes it as a pre-compile pass and replaces it
// with the inlined body or, if `fusion` is verified, a single PythonUDFOpDescV2.
//
// `ignoreUnknown = true`: the frontend stamps UI-only convenience fields (e.g.
// `macroSyncedAt` — epoch ms used to detect stale embeds against the live
// definition) into operatorProperties before persisting. The backend doesn't
// model those fields here, so Jackson would fail to deserialize the request
// without this annotation.
@JsonIgnoreProperties(ignoreUnknown = true)
class MacroOpDesc extends LogicalOp {

  @JsonProperty(value = "macroId", required = true)
  @JsonSchemaTitle("Macro ID")
  @JsonPropertyDescription("Identifier of the macro definition (workflow ID).")
  var macroId: String = ""

  @JsonProperty(value = "macroVersion")
  @JsonSchemaTitle("Macro Version")
  @JsonPropertyDescription("Pinned version (vid) of the macro definition. Used only in LIVE mode.")
  var macroVersion: Int = 0

  @JsonProperty(value = "linkMode", required = true)
  @JsonSchemaTitle("Link Mode")
  @JsonPropertyDescription("LIVE = referenced by (macroId, macroVersion); SNAPSHOT = embedded body.")
  var linkMode: String = MacroOpDesc.LIVE

  @JsonProperty(value = "snapshot")
  @JsonSchemaTitle("Snapshot")
  @JsonPropertyDescription("Embedded macro body; present only when linkMode = SNAPSHOT.")
  var snapshot: Option[MacroBody] = None

  @JsonProperty(value = "inputPortCount", required = true)
  @JsonSchemaTitle("Input Port Count")
  var inputPortCount: Int = 0

  @JsonProperty(value = "outputPortCount", required = true)
  @JsonSchemaTitle("Output Port Count")
  var outputPortCount: Int = 0

  @JsonProperty(value = "displayName")
  @JsonSchemaTitle("Display Name")
  var displayName: String = ""

  @JsonProperty(value = "fusion")
  @JsonSchemaTitle("Fusion")
  @JsonPropertyDescription(
    "AI-fused single-UDF replacement (Section 9.2). When verified, MacroExpander uses this " +
      "instead of inlining the body."
  )
  var fusion: Option[MacroFusion] = None

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ) =
    throw new IllegalStateException(
      s"MacroOpDesc[$macroId] must be expanded by MacroExpander before physical-plan " +
        s"compilation. This is a programmer error: the pre-compile expansion pass did not run."
    )

  override def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan =
    throw new IllegalStateException(
      s"MacroOpDesc[$macroId] must be expanded by MacroExpander before physical-plan " +
        s"compilation. This is a programmer error: the pre-compile expansion pass did not run."
    )

  override def operatorInfo: OperatorInfo = OperatorInfo(
    userFriendlyName = if (displayName.nonEmpty) displayName else "Macro",
    operatorDescription = "Composite operator: a reusable, encapsulated sub-workflow.",
    operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
    inputPorts = (0 until inputPortCount).toList.map(i => InputPort(PortIdentity(i))),
    outputPorts = (0 until outputPortCount).toList.map(i => OutputPort(PortIdentity(i)))
  )
}

object MacroOpDesc {
  // Link modes — strings rather than an enum to keep Jackson serialization trivial.
  val LIVE: String = "LIVE"
  val SNAPSHOT: String = "SNAPSHOT"
}
