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

package org.apache.texera.amber.operator.binning

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{
  PythonTemplateBuilderStringContext,
  pyStringLiteral
}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "attribute": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class BinningOpDesc extends PythonOperatorDescriptor with StandaloneCodeGenerator {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute")
  @JsonPropertyDescription("numeric column to cut into bins")
  @AutofillAttributeName
  var attribute: EncodableString = ""

  @JsonProperty(required = true, defaultValue = "equal width")
  @JsonSchemaTitle("Method")
  @JsonPropertyDescription("how the bins are cut")
  var method: BinningMethod = BinningMethod.EQUAL_WIDTH

  @JsonProperty(required = true, defaultValue = "4")
  @JsonSchemaTitle("Number of bins")
  @JsonPropertyDescription("how many bins to cut the column into")
  @JsonSchemaInject(json = """{"minimum": 2, "maximum": 100}""")
  var bins: Int = 4

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Binning",
      operatorDescription =
        "Cut a numeric column into bins, so rows can be grouped by range rather than by value",
      operatorGroupName = OperatorGroupConstants.CLEANING_GROUP,
      inputPorts = List(InputPort()),
      // Blocking: an equal-frequency cut is made at the column's quantiles, which
      // are not known until the last row has arrived.
      outputPorts = List(OutputPort(blocking = true))
    )

  /** The bin a row fell in, named after the column it was cut from. */
  private def resultColumn: String = s"${attribute}_bin"

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] =
    Map(
      operatorInfo.outputPorts.head.id ->
        // The bin reads as its own range, "(2.5, 5.0]", rather than as a number:
        // the point of binning is a label to group by, and a number would invite
        // arithmetic on what is really a name.
        inputSchemas.values.head.add(resultColumn, AttributeType.STRING)
    )

  /** `duplicates="drop"` because a quantile cut can put two edges in one place
    * where the values repeat, and fewer bins beats raising.
    */
  private def cutArgs: String =
    method match {
      case BinningMethod.EQUAL_WIDTH     => s"bins=$bins"
      case BinningMethod.EQUAL_FREQUENCY => s"""q=$bins, duplicates="drop""""
    }

  private def cutName: String =
    method match {
      case BinningMethod.EQUAL_WIDTH     => "pd.cut"
      case BinningMethod.EQUAL_FREQUENCY => "pd.qcut"
    }

  /** An empty cell has no bin, and `astype(str)` would render its absence as the
    * text "nan", so the hole is kept as one.
    */
  private val labelSuffix: String =
    """.astype("string").astype("object").where(lambda s: s.notna(), None)"""

  override def generatePythonCode(): String = {
    val cut = cutName
    val args = cutArgs
    val suffix = labelSuffix
    // The result column is named in PYTHON rather than here: joining it to
    // `attribute` in Scala would hand `pyb` a plain string, and the value it was
    // built to protect would be spliced into the template unguarded.
    //
    // The guard is there because edges cannot be found in a column that holds
    // nothing: an equal-width cut raises both when every cell is empty and when
    // no row arrived at all. Leaving those values uncut keeps the holes, and the
    // suffix turns them into the empty bins they already are.
    pyb"""from pytexera import *
       |import pandas as pd
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        out = table.copy()
       |        _column = $attribute
       |        _binned = out[_column]
       |        if _binned.notna().any():
       |            _binned = ${cut}(_binned, ${args})
       |        out[_column + "_bin"] = _binned${suffix}
       |        yield out""".encode
  }

  override def generateStandaloneCode(): String = {
    val column = pyStringLiteral(attribute)
    val result = pyStringLiteral(resultColumn)
    s"""out1df = in1df.copy()
       |_binned = out1df[$column]
       |if _binned.notna().any():
       |    _binned = $cutName(_binned, $cutArgs)
       |out1df[$result] = _binned$labelSuffix""".stripMargin
  }
}
