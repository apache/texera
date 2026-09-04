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

package org.apache.texera.amber.operator.visualization.boxViolinPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription, JsonPropertyOrder}
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
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder

import javax.validation.constraints.NotNull

@JsonPropertyOrder(Array("value", "quartileType", "horizontalOrientation", "violinPlot"))
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "value": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class BoxViolinPlotOpDesc extends PythonOperatorDescriptor with PlotlyStandaloneCode {

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("Data column for box plot")
  @AutofillAttributeName
  @NotNull(message = "Value Column cannot be empty")
  var value: EncodableString = ""

  @JsonProperty(
    value = "Quartile Method",
    required = true,
    defaultValue = "linear"
  )
  var quartileType: BoxViolinPlotQuartileFunction = _

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Horizontal Orientation")
  @JsonPropertyDescription("Orientation style")
  var horizontalOrientation: Boolean = _

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Violin Plot")
  @JsonPropertyDescription(
    "Check this box to overlay a violin plot on the box plot; otherwise, show only the box plot"
  )
  var violinPlot: Boolean = _

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Box/Violin Plot",
      "Visualize data using either a Box Plot or a Violin Plot. Box plots are drawn as a box with a vertical line down the middle which is mean value, and has horizontal lines attached to each side (known as “whiskers”). Violin plots provide more detail by showing a smoothed density curve on each side, and also include a box plot inside for comparison.",
      OperatorGroupConstants.VISUALIZATION_STATISTICAL_GROUP
    )

  def manipulateTable(): PythonTemplateBuilder = {
    assert(value.nonEmpty, "Value Column cannot be empty")

    pyb"""
         |        table = table.dropna(subset = [$value]) #remove missing values
         |
         |"""
  }

  def createPlotlyFigure(): PythonTemplateBuilder = {
    val horizontal = if (horizontalOrientation) "True" else "False"
    val violin = if (violinPlot) "True" else "False"
    pyb"""
       |        if($violin):
       |            if ($horizontal):
       |                fig = px.violin(table, x=$value, box=True, points='all')
       |            else:
       |                fig = px.violin(table, y=$value, box=True, points='all')
       |        else:
       |            if($horizontal):
       |                fig = px.box(table, x=$value,boxmode="overlay", points='all')
       |            else:
       |                fig = px.box(table, y=$value,boxmode="overlay", points='all')
       |        fig.update_traces(quartilemethod="${quartileType.getQuartiletype}", col=1)
       |        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
       |"""
  }

  override def generatePythonCode(): String = {

    val finalCode =
      pyb"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import pandas as pd
         |import plotly.graph_objects as go
         |import plotly.io
         |import json
         |import pickle
         |import plotly
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Box/Violin Plot is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty.")}
         |           return
         |        ${manipulateTable()}
         |        if table.empty:
         |           yield {'html-content': self.render_error("value column contains only non-positive numbers or nulls.")}
         |           return
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |        """
    finalCode.encode
  }

  // Output is a Plotly visualization, not a tabular DataFrame.
  // The translator skips it in the leaf-DataFrame print block.
  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {
    val horizontal = if (horizontalOrientation) "True" else "False"
    val violin = if (violinPlot) "True" else "False"
    val quartileMethod =
      if (quartileType == null) "linear" else quartileType.getQuartiletype
    val valueLit = pyStringLiteral(value)

    // The error page is written to output.html, the same file a plotted chart lands
    // in, so a reason for "no chart" is where the reader looks for the chart —
    // printing it to the terminal alone left output.html absent. render_error's
    // continuation line keeps the runtime path's indentation, since the HTML is
    // triple-quoted and those spaces reach the browser.
    s"""def render_error(error_msg):
       |    return '''<h1>Box/Violin Plot is not available.</h1>
       |                  <p>Reason is: {} </p>
       |               '''.format(error_msg)
       |
       |def fail(error_msg):
       |    with open("output.html", "w", encoding="utf-8") as output:
       |        output.write(render_error(error_msg))
       |    print(f"Box/Violin Plot error: {error_msg}")
       |
       |if in1df.empty:
       |    fail("input table is empty.")
       |else:
       |    # Bound to a name of its own: the same frame can feed another branch
       |    # of the plan, which must still see every row.
       |    chart_df = in1df.dropna(subset=[$valueLit])
       |    if chart_df.empty:
       |        fail("value column contains only non-positive numbers or nulls.")
       |    else:
       |        if $violin:
       |            if $horizontal:
       |                fig = px.violin(chart_df, x=$valueLit, box=True, points='all')
       |            else:
       |                fig = px.violin(chart_df, y=$valueLit, box=True, points='all')
       |        else:
       |            if $horizontal:
       |                fig = px.box(chart_df, x=$valueLit, boxmode="overlay", points='all')
       |            else:
       |                fig = px.box(chart_df, y=$valueLit, boxmode="overlay", points='all')
       |        fig.update_traces(quartilemethod=${pyStringLiteral(quartileMethod)}, col=1)
       |        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
       |        fig.write_json("output.json")
       |        fig.write_html("output.html")
       |        print("Box/Violin Plot saved to output.json and output.html")""".stripMargin
  }

}
