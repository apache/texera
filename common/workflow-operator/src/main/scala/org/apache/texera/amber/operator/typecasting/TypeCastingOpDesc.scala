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

  override def generateStandaloneCode(): String = generateStandaloneCode(Map.empty)

  override def generateStandaloneCode(inputSchemas: Map[PortIdentity, Schema]): String = {
    val units = Option(typeCastingUnits).getOrElse(List.empty)
    if (units.isEmpty) return "out1df = in1df.copy()"

    // `tupleCasting` takes a Map of column to target type and reads each column's
    // ORIGINAL value once, so two units naming one column collapse to the last.
    val lastPerColumn = units.zipWithIndex.groupBy(_._1.attribute).view.mapValues(_.last._2).toMap
    val effective = units.zipWithIndex.collect {
      case (unit, i) if lastPerColumn(unit.attribute) == i => unit
    }

    // What each column arrived as: no cast reads another's result.
    val declared: Map[String, AttributeType] = inputSchemas.values.headOption
      .map(_.getAttributes.map(a => a.getName -> a.getType).toMap)
      .getOrElse(Map.empty)

    val lines = scala.collection.mutable.ArrayBuffer[String]("out1df = in1df.copy()")
    effective.foreach { unit =>
      val colLit = pyStringLiteral(unit.attribute)
      // Every cast goes through the transcription of AttributeTypeUtils rather
      // than through Python's own conversions, which answer differently: a
      // non-empty string is always a true boolean, and `int("6.7")` raises
      // where the engine's NumberFormat reads 6.
      //
      // A timestamp is the one that stays approximate. The engine reads it with
      // DateParserUtils, which accepts a set of formats no single pandas call
      // states, so this coerces what it cannot read rather than claiming a
      // match it does not have.
      val expr = unit.resultType match {
        case AttributeType.STRING =>
          // Not `astype(str)`, which renders an empty cell as "nan" and
          // capitalises a boolean. See [[renderedAsText]].
          renderedAsText(s"out1df[$colLit]", declared.get(unit.attribute))
        case AttributeType.INTEGER =>
          // A hole survives the cast, because parseField returns a null field
          // untouched; nullable "Int64" holds one where numpy's int cannot. Only
          // a DOUBLE source saturates on the way to 32 bits, and only the
          // declared type still says so: the values reach the helper as floats.
          val wrap = !declared.get(unit.attribute).contains(AttributeType.DOUBLE)
          val wrapArg = if (wrap) "True" else "False"
          s"""out1df[$colLit].apply(lambda x: pd.NA if pd.isna(x) else _texera_cast_int32(x, $wrapArg)).astype("Int64")"""
        case AttributeType.LONG =>
          s"""out1df[$colLit].apply(lambda x: pd.NA if pd.isna(x) else _texera_cast_integral(x)).astype("Int64")"""
        case AttributeType.DOUBLE =>
          // NaN rather than pd.NA: float64 is how a double column is held here,
          // and it carries its hole as NaN. pd.NA would not survive the astype.
          s"""out1df[$colLit].apply(lambda x: float("nan") if pd.isna(x) else _texera_cast_double(x)).astype("float64")"""
        case AttributeType.BOOLEAN =>
          // Nullable "boolean" for the same reason, and because `.astype(bool)`
          // reads NaN as True: NaN is a non-zero float.
          s"""out1df[$colLit].apply(lambda x: pd.NA if pd.isna(x) else _texera_cast_boolean(x)).astype("boolean")"""
        case AttributeType.TIMESTAMP =>
          // A number is an instant in milliseconds and needs its own reading;
          // see the helper. Text keeps the parser it already had.
          if (declared.get(unit.attribute).contains(AttributeType.LONG))
            s"""_texera_epoch_millis_to_timestamp(out1df[$colLit])"""
          else s"""pd.to_datetime(out1df[$colLit], errors="coerce")"""
        case _ => s"""out1df[$colLit]"""
      }
      lines += s"""out1df[$colLit] = $expr"""
    }
    lines.mkString("\n")
  }

  override def standaloneHelpers(): Seq[String] = Seq(StandaloneHelpers.AttributeCasts)
}
