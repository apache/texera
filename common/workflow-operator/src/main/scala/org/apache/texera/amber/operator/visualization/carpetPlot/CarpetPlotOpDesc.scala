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

package org.apache.texera.amber.operator.visualization.carpetPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{
  PythonTemplateBuilderStringContext,
  pyStringLiteral
}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.visualization.PlotlyStandaloneCode
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import javax.validation.constraints.NotNull

// type constraint: a / b / y are cast with astype(float) to build the carpet grid,
// so they can only be numeric columns.
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "a": { "enum": ["integer", "long", "double"] },
    "b": { "enum": ["integer", "long", "double"] },
    "y": { "enum": ["integer", "long", "double"] }
  }
}
""")
class CarpetPlotOpDesc extends PythonOperatorDescriptor with PlotlyStandaloneCode {

  @JsonProperty(value = "a", required = true)
  @NotNull(message = "A-axis Attribute cannot be empty")
  @JsonSchemaTitle("First Parameter Axis Column")
  @JsonPropertyDescription("Column representing the first parameter axis (a)")
  @AutofillAttributeName
  var a: EncodableString = ""

  @JsonProperty(value = "b", required = true)
  @NotNull(message = "B-axis Attribute cannot be empty")
  @JsonSchemaTitle("Second Parameter Axis Column")
  @JsonPropertyDescription("Column representing the second parameter axis (b)")
  @AutofillAttributeName
  var b: EncodableString = ""

  @JsonProperty(value = "y", required = true)
  @NotNull(message = "Y Value cannot be empty")
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("Column representing the value at each (a, b) coordinate")
  @AutofillAttributeName
  var y: EncodableString = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Carpet Plot",
      "Visualize data in a Carpet Plot",
      OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP
    )

  override def generatePythonCode(): String = {
    val finalCode =
      pyb"""
           |from pytexera import *
           |import plotly.graph_objects as go
           |import plotly.io as pio
           |
           |class ProcessTableOperator(UDFTableOperator):
           |
           |    @overrides
           |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
           |
           |        if table.empty:
           |            yield {"html-content": "<h3>Input table is empty</h3>"}
           |            return
           |
           |        a_col = $a
           |        b_col = $b
           |        y_col = $y
           |
           |        for col in [a_col, b_col, y_col]:
           |            if col not in table.columns:
           |                yield {"html-content": f"<h3>Column '{col}' not found</h3>"}
           |                return
           |
           |        table = table.dropna(subset=[a_col, b_col, y_col])
           |
           |        if table.empty:
           |            yield {"html-content": "<h3>No valid rows after removing nulls</h3>"}
           |            return
           |
           |        try:
           |            table[a_col] = table[a_col].astype(float)
           |            table[b_col] = table[b_col].astype(float)
           |            table[y_col] = table[y_col].astype(float)
           |        except Exception as e:
           |            yield {"html-content": f"<h3>Error converting input columns to numeric values: {str(e)}</h3>"}
           |            return
           |
           |        try:
           |            fig = go.Figure(go.Carpet(
           |                a=table[a_col],
           |                b=table[b_col],
           |                y=table[y_col]
           |            ))
           |            html = pio.to_html(fig, include_plotlyjs='cdn', auto_play=False)
           |            yield {"html-content": html}
           |        except Exception as e:
           |            yield {"html-content": f"<h3>Error generating carpet plot: {str(e)}</h3>"}
           |"""
    finalCode.encode
  }

  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {
    val aLit = pyStringLiteral(a)
    val bLit = pyStringLiteral(b)
    val yLit = pyStringLiteral(y)
    s"""table = in1df.dropna(subset=[$aLit, $bLit, $yLit]).copy()
       |if table.empty:
       |    print("Carpet plot error: No valid rows after removing nulls")
       |else:
       |    table[$aLit] = table[$aLit].astype(float)
       |    table[$bLit] = table[$bLit].astype(float)
       |    table[$yLit] = table[$yLit].astype(float)
       |    fig = go.Figure(go.Carpet(
       |        a=table[$aLit],
       |        b=table[$bLit],
       |        y=table[$yLit]
       |    ))
       |    fig.write_json("output.json")
       |    fig.write_html("output.html")
       |    print("Carpet plot saved to output.json and output.html")""".stripMargin
  }

}
