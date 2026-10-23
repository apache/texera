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
import org.apache.texera.amber.core.tuple.Schema
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
      val colLit = pyStringLiteral(p.attribute)
      p.condition match {
        case ComparisonType.IS_NULL     => s"""(in1df[$colLit].isna())"""
        case ComparisonType.IS_NOT_NULL => s"""(in1df[$colLit].notna())"""
        case other                      =>
          // FilterPredicate finds the field through Schema, which ignores case,
          // and compares by the type declared there. Without a schema the
          // column's dtype stands in for its type.
          val kind = schema
            .flatMap(s => Try(s.getAttribute(p.attribute)).toOption)
            .map(a => pyStringLiteral(a.getType.name))
            .getOrElse("None")
          val opLit = pyStringLiteral(other.getName)
          val valueLit = pyStringLiteral(Option(p.value).getOrElse(""))
          s"_texera_filter_keep(in1df[$colLit], $opLit, $valueLit, $kind)"
      }
    }
    s"out1df = in1df[${conditions.mkString(" | ")}].reset_index(drop=True)"
  }

  override def standaloneHelpers(): Seq[String] =
    Seq(StandaloneHelpers.AttributeCasts, SpecializedFilterOpDesc.FilterPredicates)
}

object SpecializedFilterOpDesc {

  /** `_texera_filter_keep`: `FilterPredicate.evaluate`, comparing each type as the engine does.
    * STRING compares as numbers when both sides parse, else by UTF-16 code units. BOOLEAN
    * compares as lower-case text. INTEGER and DOUBLE use `Double.compareTo`, so NaN sits above
    * every number and -0.0 below 0.0. LONG reads the value with `Long.valueOf`. TIMESTAMP
    * compares whole milliseconds in the machine's zone. A null passes nothing.
    */
  val FilterPredicates: String =
    """def _texera_filter_keep(column, condition, value, kind):
      |    import math
      |    import re
      |
      |    def java_trim(text):
      |        return text.strip("".join(chr(i) for i in range(33)))
      |
      |    java_double_text = re.compile(
      |        r"[+-]?(NaN|Infinity|"
      |        r"((\d+\.?\d*|\.\d+)([eE][+-]?\d+)?"
      |        r"|0[xX]([0-9a-fA-F]+\.?|[0-9a-fA-F]*\.[0-9a-fA-F]+)[pP][+-]?\d+)[fFdD]?)",
      |        re.ASCII,
      |    )
      |
      |    def java_double(text):
      |        text = java_trim(text)
      |        if not java_double_text.fullmatch(text):
      |            raise ValueError("For input string: " + repr(text))
      |        body = text.lstrip("+-")
      |        if body == "NaN":
      |            return float("nan")
      |        if body == "Infinity":
      |            return float("-inf") if text.startswith("-") else float("inf")
      |        if text[-1] in "dDfF":
      |            text = text[:-1]
      |        if body[:2] in ("0x", "0X"):
      |            try:
      |                return float.fromhex(text)
      |            except OverflowError:
      |                return float("-inf") if text.startswith("-") else float("inf")
      |        return float(text)
      |
      |    def double_key(x):
      |        if math.isnan(x):
      |            return (1, 0.0, 0.0)
      |        return (0, x, math.copysign(1.0, x))
      |
      |    def text_key(s):
      |        return s.encode("utf-16-be", "surrogatepass")
      |
      |    def compared(left, right):
      |        return (left > right) - (left < right)
      |
      |    if kind is None:
      |        dtype = column.dtype
      |        if pd.api.types.is_bool_dtype(dtype):
      |            kind = "BOOLEAN"
      |        elif pd.api.types.is_integer_dtype(dtype):
      |            kind = "INTEGER"
      |        elif pd.api.types.is_float_dtype(dtype):
      |            kind = "DOUBLE"
      |        elif pd.api.types.is_datetime64_any_dtype(dtype):
      |            kind = "TIMESTAMP"
      |        else:
      |            kind = "STRING"
      |
      |    if kind in ("DOUBLE", "INTEGER"):
      |        target = double_key(java_double(value))
      |
      |        def order(v):
      |            return compared(double_key(float(v)), target)
      |
      |    elif kind == "LONG":
      |        text = java_trim(value)
      |        if not re.fullmatch(r"[+-]?\d+", text) or not -(2**63) <= int(text) < 2**63:
      |            raise ValueError("For input string: " + repr(text))
      |        target = int(text)
      |
      |        def order(v):
      |            return compared(int(v), target)
      |
      |    elif kind == "BOOLEAN":
      |        target = text_key(java_trim(value).lower())
      |
      |        def order(v):
      |            return compared(text_key("true" if v else "false"), target)
      |
      |    elif kind == "TIMESTAMP":
      |        zone = _texera_local_zone()
      |
      |        def wall_clock(ts):
      |            ts = pd.Timestamp(ts).floor("ms")
      |            if ts.tzinfo is None:
      |                return ts
      |            return pd.Timestamp(_texera_local_wall_clock(ts.to_pydatetime(), zone))
      |
      |        target = wall_clock(java_trim(value))
      |
      |        def order(v):
      |            return compared(wall_clock(v), target)
      |
      |    else:
      |        try:
      |            value_number = double_key(java_double(value))
      |        except ValueError:
      |            value_number = None
      |
      |        def order(v):
      |            text = str(v)
      |            if value_number is not None:
      |                try:
      |                    return compared(double_key(java_double(text)), value_number)
      |                except ValueError:
      |                    pass
      |            return compared(text_key(text), text_key(value))
      |
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
      |    )""".stripMargin
}
