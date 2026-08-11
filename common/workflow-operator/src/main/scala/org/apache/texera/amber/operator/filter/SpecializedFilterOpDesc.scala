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

package org.apache.texera.amber.operator.filter

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

class SpecializedFilterOpDesc extends FilterOpDesc with StandaloneCodeGenerator {

  @JsonProperty(value = "predicates", required = true)
  @JsonPropertyDescription("multiple predicates in OR")
  var predicates: List[FilterPredicate] = List.empty

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
          "org.apache.texera.amber.operator.filter.SpecializedFilterOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Filter",
      "Performs a filter operation using OR between multiple predicates",
      OperatorGroupConstants.CLEANING_GROUP,
      List(InputPort()),
      List(OutputPort()),
      supportReconfiguration = true
    )
  }

  override def generateStandaloneCode(): String = {
    if (predicates.isEmpty) return "out1df = in1df.copy()"
    val conditions = predicates.map { p =>
      val colLit = pyStringLiteral(p.attribute)
      p.condition match {
        case ComparisonType.IS_NULL     => s"""(in1df[$colLit].isna())"""
        case ComparisonType.IS_NOT_NULL => s"""(in1df[$colLit].notna())"""
        case other =>
          val op = other.getName // returns "=", ">=", "<", etc. (see ComparisonType.java)
          val pyOp = if (op == "=") "==" else op
          // notna mirrors FilterPredicate, which answers false for every condition
          // but IS_NULL / IS_NOT_NULL once the field is null. Only `!=` needs it —
          // pandas answers True there, where every other operator answers False —
          // but guarding all of them keeps the one rule visible in one place.
          s"""(in1df[$colLit].notna() & (in1df[$colLit] $pyOp ${coerceValue(p.value)}))"""
      }
    }
    s"out1df = in1df[${conditions.mkString(" | ")}].reset_index(drop=True)"
  }

  // Try numeric coercion so generated code compares column values against the right type.
  // Strings that don't parse fall through to a quoted string literal.
  private def coerceValue(raw: String): String = {
    try {
      raw.toInt.toString
    } catch {
      case _: NumberFormatException =>
        try {
          raw.toDouble.toString
        } catch {
          case _: NumberFormatException =>
            pyStringLiteral(raw)
        }
    }
  }
}
