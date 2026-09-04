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
package org.apache.texera.amber.operator.visualization.timeSeriesplot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.visualization.PlotlyStandaloneCode
import org.apache.texera.amber.operator.metadata.annotations.{AutofillAttributeName, SampleColumn}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral

import javax.validation.constraints.{NotBlank, NotNull}

// type constraint: the time axis is an instant and the plotted value is a measurement.
// Neither is enforced today: a text value column silently degenerates the y axis into
// a categorical one. The time axis also accepts a string, because pd.to_datetime
// parses date text and a CSV source hands its dates over as one; a type rule cannot
// tell such text from an unparseable value, so that half is left to the coercion,
// which drops what it cannot read.
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "timeColumn": { "enum": ["timestamp", "string"] },
    "valueColumn": { "enum": ["integer", "long", "double"] }
  }
}
""")
class TimeSeriesOpDesc extends PythonOperatorDescriptor with PlotlyStandaloneCode {

  @JsonProperty(value = "timeColumn", required = true)
  @JsonSchemaTitle("Time Column")
  @JsonPropertyDescription("The column containing time/date values (e.g., Date, Timestamp).")
  @AutofillAttributeName
  @SampleColumn("start_ts")
  @NotNull(message = "Time Column cannot be empty")
  var timeColumn: EncodableString = ""

  @JsonProperty(value = "valueColumn", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("The numerical column to plot on the Y-axis (e.g., Sales, Temperature).")
  @JsonSchemaInject(json = """{"enum": "autofill"}""")
  @AutofillAttributeName
  @NotNull(message = "Value Column cannot be empty")
  var valueColumn: EncodableString = ""

  @JsonProperty(value = "categoryColumn", required = false, defaultValue = "No Selection")
  @JsonSchemaTitle("Category Column")
  @JsonPropertyDescription("Optional - A categorical column to create separate lines.")
  @AutofillAttributeName
  @SampleColumn("node_src")
  var CategoryColumn: EncodableString = "No Selection"

  @JsonProperty(value = "facetColumn", required = false, defaultValue = "No Selection")
  @JsonSchemaTitle("Facet Column")
  @JsonPropertyDescription("Optional - A column to create separate subplots.")
  @AutofillAttributeName
  @SampleColumn("node_dst")
  var facetColumn: EncodableString = "No Selection"

  // Declared as a schema enum rather than named only in the description: the code
  // below reads exactly two values (`plotType == "area"`, else a line), so a free-text
  // box let a typo through and silently drew a line -- no error, just not the chart
  // that was asked for.
  @JsonProperty(value = "line", defaultValue = "line", required = true)
  @JsonSchemaTitle("Plot Type")
  @JsonPropertyDescription("Select the type of time series plot (line, area).")
  @JsonSchemaInject(json = """{ "enum": ["line", "area"], "default": "line" }""")
  @NotBlank(message = "Plot Type cannot be empty")
  var plotType: String = "line"

  @JsonProperty(value = "slider", defaultValue = "false")
  @JsonSchemaTitle("Show Range Slider")
  @JsonPropertyDescription("Display a range slider at the bottom of the plot.")
  var showRangeSlider: Boolean = _

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema().add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Time Series Plot",
      "Visualize trends and patterns over time.",
      OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    )

  override def generatePythonCode(): String = {
    val dropnaCols = List(timeColumn, valueColumn) ++
      (if (CategoryColumn != "No Selection") Some(CategoryColumn) else None) ++
      (if (facetColumn != "No Selection") Some(facetColumn) else None)
    val dropnaStr = dropnaCols.map(c => pyb"$c").mkString("[", ", ", "]")

    val colorArg = if (CategoryColumn != "No Selection") pyb", color=$CategoryColumn" else ""
    val facetArg = if (facetColumn != "No Selection") pyb", facet_col=$facetColumn" else ""
    val plotFunc = if (plotType == "area") "px.area" else "px.line"
    val showSlider = if (showRangeSlider) "True" else "False"

    pyb"""
       |from pytexera import *
       |import plotly.express as px
       |import plotly.io
       |import pandas as pd
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    def render_error(self, msg) -> str:
       |        return f"<h1>Time Series Plot is not available.</h1><p>Reason: {msg}</p>"
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int):
       |        if table.empty:
       |            yield {'html-content': self.render_error("Input table is empty.")}
       |            return
       |
       |        try:
       |            table[$timeColumn] = pd.to_datetime(table[$timeColumn], errors='coerce')
       |            table = table.dropna(subset=$dropnaStr).sort_values(by=$timeColumn)
       |
       |            if table.empty:
       |                yield {'html-content': self.render_error("Table became empty after filtering.")}
       |                return
       |
       |            fig = $plotFunc(table, x=$timeColumn, y=$valueColumn$colorArg$facetArg)
       |
       |            if $showSlider:
       |                fig.update_xaxes(rangeslider_visible=True)
       |
       |            fig.update_layout(
       |                margin=dict(l=0, r=0, t=30, b=0),
       |                title=dict(text="Time Series Plot", x=0.5),
       |                xaxis_title=$timeColumn,
       |                yaxis_title=$valueColumn,
       |                template="plotly_white"
       |            )
       |
       |            html = plotly.io.to_html(fig, include_plotlyjs='cdn', full_html=False)
       |            yield {'html-content': html}
       |
       |        except Exception as e:
       |            yield {'html-content': self.render_error(str(e))}
       |""".encode
  }

  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {
    val dropnaCols = List(timeColumn, valueColumn) ++
      (if (CategoryColumn != "No Selection") Some(CategoryColumn) else None) ++
      (if (facetColumn != "No Selection") Some(facetColumn) else None)
    val dropnaStr = dropnaCols.map(pyStringLiteral).mkString("[", ", ", "]")
    val colorArg =
      if (CategoryColumn != "No Selection") s""", color=${pyStringLiteral(CategoryColumn)}"""
      else ""
    val facetArg =
      if (facetColumn != "No Selection") s""", facet_col=${pyStringLiteral(facetColumn)}""" else ""
    val timeLit = pyStringLiteral(timeColumn)
    val valueLit = pyStringLiteral(valueColumn)
    val plotFunc = if (plotType == "area") "px.area" else "px.line"
    val showSlider = if (showRangeSlider) "True" else "False"

    s"""def render_error(msg):
       |    return f"<h1>Time Series Plot is not available.</h1><p>Reason: {msg}</p>"
       |
       |if in1df.empty:
       |    with open("output.html", "w", encoding="utf-8") as output:
       |        output.write(render_error("Input table is empty."))
       |else:
       |    try:
       |        table = in1df.copy()
       |        table[$timeLit] = pd.to_datetime(table[$timeLit], errors='coerce')
       |        table = table.dropna(subset=$dropnaStr).sort_values(by=$timeLit)
       |        if table.empty:
       |            with open("output.html", "w", encoding="utf-8") as output:
       |                output.write(render_error("Table became empty after filtering."))
       |        else:
       |            fig = $plotFunc(table, x=$timeLit, y=$valueLit$colorArg$facetArg)
       |            if $showSlider:
       |                fig.update_xaxes(rangeslider_visible=True)
       |            fig.update_layout(
       |                margin=dict(l=0, r=0, t=30, b=0),
       |                title=dict(text="Time Series Plot", x=0.5),
       |                xaxis_title=$timeLit,
       |                yaxis_title=$valueLit,
       |                template="plotly_white"
       |            )
       |            fig.write_json("output.json")
       |            fig.write_html("output.html")
       |            print("Time series plot saved to output.html")
       |    except Exception as e:
       |        with open("output.html", "w", encoding="utf-8") as output:
       |            output.write(render_error(str(e)))""".stripMargin
  }

}
