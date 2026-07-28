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

package org.apache.texera.amber.operator.visualization.tablesChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder

import javax.validation.constraints.NotEmpty
class TablesPlotOpDesc extends PythonOperatorDescriptor with StandaloneCodeGenerator {

  @JsonPropertyDescription("List of columns to include in the table chart")
  @JsonProperty(value = "add attribute", required = true)
  @NotEmpty(message = "Included Columns cannot be empty")
  var includedColumns: List[TablesConfig] = List()

  private def getAttributes: String =
    // Join with a plain comma: each column renders to a decode_python_template(...)
    // call, so joining with the literal ',' would put a string right after a call
    // and produce invalid Python.
    includedColumns.map(c => pyb"""${c.attributeName}""").mkString(",")

  def manipulateTable(): PythonTemplateBuilder = {
    assert(includedColumns.nonEmpty, "Included Columns cannot be empty")
    val attributes = getAttributes
    pyb"""
       |        # drops rows with missing values pertaining to relevant columns
       |        table = table.dropna(subset=[$attributes])
       |
       |"""
  }

  def createPlotlyFigure(): PythonTemplateBuilder = {
    assert(includedColumns.nonEmpty, "Included Columns cannot be empty")
    val attributes = getAttributes
    pyb"""
         |
         |        filtered_table = table[[$attributes]]
         |        headers = filtered_table.columns.tolist()
         |        cell_values = [filtered_table[col].tolist() for col in headers]
         |
         |        fig = go.Figure(data=[go.Table(
         |            header=dict(values=headers),
         |            cells=dict(values=cell_values)
         |        )])
         |
         |
         |"""
  }

  override def generatePythonCode(): String = {
    pyb"""
       |from pytexera import *
       |import plotly.graph_objects as go
       |import plotly.io
       |class TableChartOperator(UDFTableOperator):
       |
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |
       |        if table.empty:
       |           yield {'html-content': self.render_error("input table is empty.")}
       |           return
       |
       |        ${manipulateTable()}
       |
       |        if table.empty:
       |           yield {'html-content': self.render_error("value column contains only non-positive numbers or nulls.")}
       |           return

       |
       |        ${createPlotlyFigure()}
       |        fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
       |        html_content = plotly.io.to_html(fig, include_plotlyjs='cdn')
       |        yield {'html-content': html_content}
    """.encode
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo.forVisualization(
      "Tables Plot",
      "Visualize data in a table chart.",
      OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    )
  }

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {
    assert(includedColumns.nonEmpty)
    // Mirror getAttributes: a Python list literal of the selected column names.
    val columnsList =
      includedColumns.map(c => s""""${c.attributeName}"""").mkString("[", ", ", "]")
    // The two guards mirror generatePythonCode's, which reports both conditions
    // rather than rendering a table with no rows in it.
    s"""def render_error(error_msg):
       |    return '''<h1>Tables Plot is not available.</h1>
       |              <p>Reason is: {} </p>
       |           '''.format(error_msg)
       |
       |attributes = $columnsList
       |if in1df.empty:
       |    with open("output.html", "w", encoding="utf-8") as output:
       |        output.write(render_error("input table is empty."))
       |else:
       |    table = in1df.dropna(subset=attributes)
       |    if table.empty:
       |        with open("output.html", "w", encoding="utf-8") as output:
       |            output.write(render_error("value column contains only non-positive numbers or nulls."))
       |    else:
       |        filtered_table = table[attributes]
       |        headers = filtered_table.columns.tolist()
       |        cell_values = [filtered_table[col].tolist() for col in headers]
       |
       |        fig = go.Figure(data=[go.Table(
       |            header=dict(values=headers),
       |            cells=dict(values=cell_values)
       |        )])
       |        fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
       |        fig.write_json("output.json")
       |        fig.write_html("output.html")
       |        print("Tables plot saved to output.json")""".stripMargin
  }
}
