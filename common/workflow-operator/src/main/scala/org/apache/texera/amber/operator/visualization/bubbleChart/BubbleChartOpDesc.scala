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

package org.apache.texera.amber.operator.visualization.bubbleChart

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
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder

import javax.validation.constraints.NotNull

/**
  * Visualization Operator to visualize results as a Bubble Chart
  * User specifies 2 columns to use for the x, y labels. Size of bubbles determined via
  * third column of data. Bubbles can be sorted via color using a fourth column.
  */

// type can be numerical only
// The z column is the bubble size, which plotly express divides by a scale
// factor, so text aborts the run. The x and y axes are positions and take any
// type, the way a scatter plot's do.
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "zValue": { "enum": ["integer", "long", "double"] }
  }
}
""")
class BubbleChartOpDesc extends PythonOperatorDescriptor with PlotlyStandaloneCode {

  @JsonProperty(value = "xValue", required = true)
  @JsonSchemaTitle("X-Column")
  @JsonPropertyDescription("Data column for the x-axis")
  @AutofillAttributeName
  @NotNull(message = "X-Column cannot be empty")
  var xValue: EncodableString = ""

  @JsonProperty(value = "yValue", required = true)
  @JsonSchemaTitle("Y-Column")
  @JsonPropertyDescription("Data column for the y-axis")
  @AutofillAttributeName
  @NotNull(message = "Y-Column cannot be empty")
  var yValue: EncodableString = ""

  @JsonProperty(value = "zValue", required = true)
  @JsonSchemaTitle("Z-Column")
  @JsonPropertyDescription("Data column to determine bubble size")
  @AutofillAttributeName
  @NotNull(message = "Z-Column cannot be empty")
  var zValue: EncodableString = ""

  @JsonProperty(value = "enableColor", defaultValue = "false")
  @JsonSchemaTitle("Enable Color")
  @JsonPropertyDescription("Colors bubbles using a data column")
  @JsonSchemaInject(json = """{"toggleHidden" : ["colorCategory"]}""")
  var enableColor: Boolean = false

  @JsonProperty(value = "colorCategory", required = false)
  @JsonSchemaTitle("Color-Column")
  @JsonPropertyDescription(
    "Optional data column to color bubbles with; leave empty for uniform bubbles"
  )
  @AutofillAttributeName
  var colorCategory: EncodableString = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Bubble Chart",
      "a 3D Scatter Plot; Bubbles are graphed using x and y labels, and their sizes determined by a z-value.",
      OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    )

  def manipulateTable(): PythonTemplateBuilder = {
    assert(xValue.nonEmpty, "X-Column cannot be empty")
    assert(yValue.nonEmpty, "Y-Column cannot be empty")
    assert(zValue.nonEmpty, "Z-Column cannot be empty")
    pyb"""
       |        # drops rows with missing values pertaining to relevant columns
       |        table.dropna(subset=[$xValue, $yValue, $zValue], inplace = True)
       |
       |"""
  }

  def createPlotlyFigure(): PythonTemplateBuilder = {
    assert(xValue.nonEmpty, "X-Column cannot be empty")
    assert(yValue.nonEmpty, "Y-Column cannot be empty")
    assert(zValue.nonEmpty, "Z-Column cannot be empty")
    // An unset column counts as "no color" even with the toggle on, else px.scatter(color='') fails.
    val colorArg = if (enableColor && colorCategory.nonEmpty) pyb", color=$colorCategory" else pyb""
    pyb"""
         |        fig = go.Figure(px.scatter(table, x=$xValue, y=$yValue, size=$zValue$colorArg, size_max=100))
         |"""
  }

  override def generatePythonCode(): String = {
    val finalCode =
      pyb"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import plotly.graph_objects as go
         |import plotly.io
         |import numpy as np
         |
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    def render_error(self, error_msg):
         |        return '''<h1>TreeMap is not available.</h1>
         |                  <p>Reasons are: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("Input table is empty.")}
         |            return
         |        ${manipulateTable()}
         |        ${createPlotlyFigure()}
         |        if table.empty:
         |            yield {'html-content': self.render_error("No valid rows left (every row has at least 1 missing value).")}
         |            return
         |        fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
         |        html = plotly.io.to_html(fig, include_plotlyjs = 'cdn', auto_play = False)
         |        yield {'html-content':html}
         |"""
    finalCode.encode
  }

  // Output is an HTML chart, not a tabular DataFrame.
  // The translator skips it in the leaf-DataFrame print block.
  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {
    // Same rule as the platform path: an unset column is "no color" even with the
    // toggle on, else px.scatter(color='') fails.
    val colorArg =
      if (enableColor && colorCategory.nonEmpty) s""", color=${pyStringLiteral(colorCategory)}"""
      else ""
    val xLit = pyStringLiteral(xValue)
    val yLit = pyStringLiteral(yValue)
    val zLit = pyStringLiteral(zValue)

    // The error page is written to output.html, the same file a plotted chart lands
    // in, so a reason for "no chart" is where the reader looks for the chart —
    // printing it to the terminal alone left output.html absent. The heading is the
    // runtime path's, TreeMap and all, so both paths say the same thing.
    // render_error's continuation line keeps the runtime path's indentation, since
    // the HTML is triple-quoted and those spaces reach the browser.
    s"""def render_error(error_msg):
       |    return '''<h1>TreeMap is not available.</h1>
       |                  <p>Reasons are: {} </p>
       |               '''.format(error_msg)
       |
       |def fail(error_msg):
       |    with open("output.html", "w", encoding="utf-8") as output:
       |        output.write(render_error(error_msg))
       |    print(f"Bubble chart error: {error_msg}")
       |
       |if in1df.empty:
       |    fail("Input table is empty.")
       |else:
       |    # Bound to a name of its own: the same frame can feed another branch
       |    # of the plan, which must still see every row.
       |    chart_df = in1df.dropna(subset=[$xLit, $yLit, $zLit])
       |    if chart_df.empty:
       |        fail("No valid rows left (every row has at least 1 missing value).")
       |    else:
       |        fig = go.Figure(px.scatter(
       |            chart_df,
       |            x=$xLit,
       |            y=$yLit,
       |            size=$zLit,
       |            size_max=100$colorArg
       |        ))
       |        fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
       |        fig.write_json("output.json")
       |        fig.write_html("output.html")
       |        print("Bubble chart saved to output.html")""".stripMargin
  }
}
