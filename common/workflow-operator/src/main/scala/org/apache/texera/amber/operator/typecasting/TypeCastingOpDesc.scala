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
import org.apache.texera.amber.operator.{StandaloneCodeGenerator, StandaloneHelpers}
import org.apache.texera.amber.operator.map.MapOpDesc
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
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
      val colLit = pyStringLiteral(unit.attribute)
      // Every cast goes through the transcription of AttributeTypeUtils rather
      // than through Python's own conversions, which answer differently: a
      // non-empty string is always a true boolean, and a coercing numeric cast
      // reads "6.7" as an integer the engine refuses.
      //
      // A timestamp is the one that stays approximate. The engine reads it with
      // DateParserUtils, which accepts a set of formats no single pandas call
      // states, so this coerces what it cannot read rather than claiming a
      // match it does not have.
      val expr = unit.resultType match {
        case AttributeType.STRING =>
          // `astype(str)` gets three things wrong against `toString`: an empty
          // cell renders as the text "nan", a column holding one is a float by
          // then so 6 reads "6.0", and a boolean capitalises. Each is handled
          // rather than the column cast wholesale.
          s"""out1df[$colLit].apply(""" +
            """lambda x: None if pd.isna(x) """ +
            """else ("true" if x else "false") if isinstance(x, bool) """ +
            """else str(int(x)) if isinstance(x, float) and x.is_integer() """ +
            """else str(x))"""
        case AttributeType.INTEGER | AttributeType.LONG =>
          // A hole survives the cast, because parseField returns a null field
          // untouched; pandas' nullable "Int64" holds one where numpy's int
          // cannot.
          s"""out1df[$colLit].apply(lambda x: pd.NA if pd.isna(x) else _texera_cast_integral(x)).astype("Int64")"""
        case AttributeType.DOUBLE =>
          // NaN rather than pd.NA: float64 is how a double column is held here,
          // and it carries its hole as NaN. pd.NA would not survive the astype.
          s"""out1df[$colLit].apply(lambda x: float("nan") if pd.isna(x) else _texera_cast_double(x)).astype("float64")"""
        case AttributeType.BOOLEAN =>
          // Nullable "boolean" for the same reason, and because `.astype(bool)`
          // reads NaN as True: NaN is a non-zero float.
          s"""out1df[$colLit].apply(lambda x: pd.NA if pd.isna(x) else _texera_cast_boolean(x)).astype("boolean")"""
        case AttributeType.TIMESTAMP => s"""pd.to_datetime(out1df[$colLit], errors="coerce")"""
        case _                       => s"""out1df[$colLit]"""
      }
      lines += s"""out1df[$colLit] = $expr"""
    }
    lines.mkString("\n")
  }

  override def standaloneHelpers(): Seq[String] = Seq(StandaloneHelpers.AttributeCasts)
}
