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

package org.apache.texera.common.compiler.model

import org.apache.texera.amber.operator.LogicalOp
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonIgnoreProperties, JsonInclude}
import com.fasterxml.jackson.databind.JsonNode
import org.apache.texera.common.compiler.{MacroDraft, MacroExpansion}
import org.apache.texera.amber.core.workflow.WorkflowGraphIDs
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity

@JsonIgnoreProperties(Array("expandedMacros", "drafts"))
case class LogicalPlanPojo(
    operators: List[LogicalOp],
    links: List[LogicalLink],
    opsToViewResult: List[String],
    opsToReuseResult: List[String],
    @JsonInclude(JsonInclude.Include.NON_ABSENT) macroContent: Option[JsonNode] = None,
    @JsonInclude(JsonInclude.Include.NON_ABSENT) executeOperatorIds: Option[List[String]] = None,
    // Preserve diagnostic paths through normalization and sub-DAG selection; never persist them.
    expandedMacros: Option[MacroExpansion] = None,
    drafts: List[MacroDraft] = Nil
) {
  @JsonIgnore lazy val macroExpansion: Option[MacroExpansion] =
    expandedMacros.orElse(macroContent.map(MacroExpansion.expand(_, drafts)))

  /** The first public output is the result of a selected macro; keep its native port identity. */
  @JsonIgnore lazy val macroResultPorts: Map[String, (OperatorIdentity, PortIdentity)] =
    macroExpansion.toList
      .flatMap(_.resultTargets)
      .collect {
        case (id, target) if opsToViewResult.contains(id) =>
          id -> (OperatorIdentity(target.operatorID), PortIdentity(target.portIndex))
      }
      .toMap

  @JsonIgnore lazy val normalized: LogicalPlanPojo = {
    WorkflowGraphIDs.validateIDs(operators.map(_.operatorIdentifier.id))
    macroContent
      .flatMap(_ => macroExpansion)
      .map(MacroExpansion.logicalPlan(this, _))
      .getOrElse(this)
  }
}
