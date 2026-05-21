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

package org.apache.texera.amber.operator.typecasting

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, AttributeTypeUtils, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.map.MapOpDesc
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

class TypeCastingOpDesc extends MapOpDesc with StandaloneCodeGenerator {

  @JsonProperty(required = true)
  @JsonSchemaTitle("TypeCasting Units")
  @JsonPropertyDescription("Multiple type castings")
  var typeCastingUnits: List[TypeCastingUnit] = List.empty

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    if (typeCastingUnits == null) typeCastingUnits = List.empty
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.typecasting.TypeCastingOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc { inputSchemas: Map[PortIdentity, Schema] =>
          val outputSchema = typeCastingUnits.foldLeft(inputSchemas.values.head) { (schema, unit) =>
            AttributeTypeUtils.SchemaCasting(schema, unit.attribute, unit.resultType)
          }
          Map(operatorInfo.outputPorts.head.id -> outputSchema)
        }
      )
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Type Casting",
      "Cast between types",
      OperatorGroupConstants.CLEANING_GROUP,
      List(InputPort()),
      List(OutputPort())
    )
  }

  override def generateStandaloneCode(): String = {
    val units = Option(typeCastingUnits).getOrElse(List.empty)
    if (units.isEmpty) return "out1df = in1df.copy()"

    val lines = scala.collection.mutable.ArrayBuffer[String]("out1df = in1df.copy()")
    units.foreach { unit =>
      val col = unit.attribute
      // Use pd.to_numeric / pd.to_datetime with errors="coerce" so unparseable
      // values become NaN/NaT instead of raising — matches a best-effort
      // standalone reproduction of Texera's per-row cast.
      val expr = unit.resultType match {
        case AttributeType.STRING => s"""out1df["$col"].astype(str)"""
        case AttributeType.INTEGER | AttributeType.LONG =>
          // Match JVM AttributeTypeUtils.parseInteger, which casts Double via
          // `.toInt` (truncate toward zero). pandas .astype("Int64") on a float
          // with non-integer values raises TypeError, so truncate explicitly
          // via int() while preserving NaN as pd.NA.
          s"""pd.to_numeric(out1df["$col"], errors="coerce").apply(lambda x: pd.NA if pd.isna(x) else int(x)).astype("Int64")"""
        case AttributeType.DOUBLE =>
          s"""pd.to_numeric(out1df["$col"], errors="coerce").astype("float64")"""
        case AttributeType.BOOLEAN   => s"""out1df["$col"].astype(bool)"""
        case AttributeType.TIMESTAMP => s"""pd.to_datetime(out1df["$col"], errors="coerce")"""
        case _                       => s"""out1df["$col"]"""
      }
      lines += s"""out1df["$col"] = $expr"""
    }
    lines.mkString("\n")
  }
}
