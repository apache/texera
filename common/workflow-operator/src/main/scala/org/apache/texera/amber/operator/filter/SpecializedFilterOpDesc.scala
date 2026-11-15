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
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp, PortIdentity}
import org.apache.texera.amber.operator.{StandaloneCodeGenerator, StandaloneHelpers}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.util.Try

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

  override def generateStandaloneCode(): String = generateStandaloneCode(Map.empty)

  override def generateStandaloneCode(inputSchemas: Map[PortIdentity, Schema]): String = {
    // No predicate keeps no row: the executor's filter is `predicates.exists`,
    // which answers false on an empty list. Passing the frame through would be
    // the opposite answer.
    if (predicates.isEmpty) return "out1df = in1df.iloc[0:0].copy()"
    val schema = inputSchemas.values.headOption
    val conditions = predicates.map { p =>
      // FilterPredicate reads the field through Schema, which ignores case. See
      // [[columnAsStored]].
      val colLit = pyStringLiteral(columnAsStored(p.attribute, schema))
      p.condition match {
        case ComparisonType.IS_NULL     => s"""(in1df[$colLit].isna())"""
        case ComparisonType.IS_NOT_NULL => s"""(in1df[$colLit].notna())"""
        case other                      =>
          // FilterPredicate finds the field through Schema, which ignores case,
          // and compares by the type declared there. Without a schema the
          // column's dtype stands in for its type.
          val keep = schema
            .flatMap(s => Try(s.getAttribute(p.attribute)).toOption)
            .map(a => SpecializedFilterOpDesc.keepFor(a.getType))
            .getOrElse("_texera_filter_keep")
          val opLit = pyStringLiteral(other.getName)
          val valueLit = pyStringLiteral(Option(p.value).getOrElse(""))
          s"$keep(in1df[$colLit], $opLit, $valueLit)"
      }
    }
    s"out1df = in1df[${conditions.mkString(" | ")}].reset_index(drop=True)"
  }

  override def standaloneHelpers(): Seq[String] =
    Seq(StandaloneHelpers.AttributeCasts, SpecializedFilterOpDesc.FilterPredicates)
}

object SpecializedFilterOpDesc {

  /** The function that compares a column of `attributeType`, so a script calls only the one
    * its column needs.
    */
  def keepFor(attributeType: AttributeType): String =
    attributeType match {
      case AttributeType.INTEGER | AttributeType.DOUBLE => "_texera_filter_keep_double"
      case AttributeType.LONG                           => "_texera_filter_keep_long"
      case AttributeType.BOOLEAN                        => "_texera_filter_keep_boolean"
      case AttributeType.TIMESTAMP                      => "_texera_filter_keep_timestamp"
      case _                                            => "_texera_filter_keep_string"
    }

  /** `FilterPredicate.evaluate`, one function per type the engine compares apart. A null passes
    * nothing.
    *
    *  - `_texera_filter_keep_double`: INTEGER and DOUBLE by `Double.compareTo`, NaN above all.
    *  - `_texera_filter_keep_long`: LONG, the value read by `Long.valueOf`.
    *  - `_texera_filter_keep_boolean`: BOOLEAN as lower-case text.
    *  - `_texera_filter_keep_timestamp`: TIMESTAMP by whole milliseconds in the machine's zone.
    *  - `_texera_filter_keep_string`: STRING as numbers when both parse, else by UTF-16 units.
    *  - `_texera_filter_keep`: the type read off the column's dtype, when no schema says it.
    *
    * The value is read with `_texera_java_trim`, `_texera_java_double` and `_texera_parse_int`,
    * and a double is ordered by `_texera_double_key`, all from
    * [[StandaloneHelpers.AttributeCasts]].
    */
  val FilterPredicates: String =
    """def _texera_filter_text_key(s):
      |    return s.encode("utf-16-be", "surrogatepass")
      |
      |def _texera_filter_compared(left, right):
      |    return (left > right) - (left < right)
      |
      |def _texera_filter_rows(column, condition, order):
      |    passes = {
      |        "=": lambda c: c == 0,
      |        ">": lambda c: c > 0,
      |        ">=": lambda c: c >= 0,
      |        "<": lambda c: c < 0,
      |        "<=": lambda c: c <= 0,
      |        "!=": lambda c: c != 0,
      |    }[condition]
      |    return pd.Series(
      |        [
      |            False if missing else passes(order(v))
      |            for v, missing in zip(column, column.isna())
      |        ],
      |        index=column.index,
      |        dtype=bool,
      |    )
      |
      |def _texera_filter_keep_double(column, condition, value):
      |    target = _texera_double_key(_texera_java_double(value))
      |
      |    def order(v):
      |        return _texera_filter_compared(_texera_double_key(float(v)), target)
      |
      |    return _texera_filter_rows(column, condition, order)
      |
      |def _texera_filter_keep_long(column, condition, value):
      |    target = _texera_parse_int(value, 64)
      |
      |    def order(v):
      |        return _texera_filter_compared(int(v), target)
      |
      |    return _texera_filter_rows(column, condition, order)
      |
      |def _texera_filter_keep_boolean(column, condition, value):
      |    target = _texera_filter_text_key(_texera_java_trim(value).lower())
      |
      |    def order(v):
      |        return _texera_filter_compared(_texera_filter_text_key("true" if v else "false"), target)
      |
      |    return _texera_filter_rows(column, condition, order)
      |
      |def _texera_filter_keep_timestamp(column, condition, value):
      |    zone = _texera_local_zone()
      |
      |    def wall_clock(ts):
      |        ts = pd.Timestamp(ts).floor("ms")
      |        if ts.tzinfo is None:
      |            return ts
      |        return pd.Timestamp(_texera_local_wall_clock(ts.to_pydatetime(), zone))
      |
      |    target = wall_clock(_texera_java_trim(value))
      |
      |    def order(v):
      |        return _texera_filter_compared(wall_clock(v), target)
      |
      |    return _texera_filter_rows(column, condition, order)
      |
      |def _texera_filter_keep_string(column, condition, value):
      |    try:
      |        value_number = _texera_double_key(_texera_java_double(value))
      |    except ValueError:
      |        value_number = None
      |
      |    def order(v):
      |        text = str(v)
      |        if value_number is not None:
      |            try:
      |                number = _texera_double_key(_texera_java_double(text))
      |                return _texera_filter_compared(number, value_number)
      |            except ValueError:
      |                pass
      |        return _texera_filter_compared(
      |            _texera_filter_text_key(text), _texera_filter_text_key(value)
      |        )
      |
      |    return _texera_filter_rows(column, condition, order)
      |
      |def _texera_filter_keep(column, condition, value):
      |    dtype = column.dtype
      |    if pd.api.types.is_bool_dtype(dtype):
      |        return _texera_filter_keep_boolean(column, condition, value)
      |    if pd.api.types.is_integer_dtype(dtype) or pd.api.types.is_float_dtype(dtype):
      |        return _texera_filter_keep_double(column, condition, value)
      |    if pd.api.types.is_datetime64_any_dtype(dtype):
      |        return _texera_filter_keep_timestamp(column, condition, value)
      |    return _texera_filter_keep_string(column, condition, value)""".stripMargin
}
