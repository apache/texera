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
package edu.uci.ics.amber.operator.visualization.timeSeriesplot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

class TimeSeriesOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "timeColumn", required = true)
  @JsonSchemaTitle("Time Column")
  @JsonPropertyDescription("The column containing time/date values (e.g., Date, Timestamp).")
  @AutofillAttributeName
  var timeColumn: String = ""

  @JsonProperty(value = "valueColumn", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("The numerical column to plot on the Y-axis (e.g., Sales, Temperature).")
  @JsonSchemaInject(json = """{"enum": "autofill"}""")
  @AutofillAttributeName
  var valueColumn: String = ""

  @JsonProperty(value = "categoryColumn", required = false, defaultValue = "No Selection")
  @JsonSchemaTitle("Category Column")
  @JsonPropertyDescription("Optional - A categorical column to create separate lines.")
  @AutofillAttributeName
  var CategoryColumn: String = "No Selection"

  @JsonProperty(value = "facetColumn", required = false, defaultValue = "No Selection")
  @JsonSchemaTitle("Facet Column")
  @JsonPropertyDescription("Optional - A column to create separate subplots.")
  @AutofillAttributeName
  var facetColumn: String = "No Selection"

  @JsonProperty(value = "line", defaultValue = "line", required = true)
  @JsonSchemaTitle("Plot Type")
  @JsonPropertyDescription("Select the type of time series plot (line, area).")
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
    OperatorInfo(
      "Time Series Plot",
      "Visualize trends and patterns over time.",
      OperatorGroupConstants.VISUALIZATION_BASIC_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  override def generatePythonCode(): String = {
    val dropnaCols = List(timeColumn, valueColumn) ++
      (if (CategoryColumn != "No Selection") Some(CategoryColumn) else None) ++
      (if (facetColumn != "No Selection") Some(facetColumn) else None)
    val dropnaStr = dropnaCols.map(c => s"'$c'").mkString("[", ", ", "]")

    val colorArg = if (CategoryColumn != "No Selection") s", color='$CategoryColumn'" else ""
    val facetArg = if (facetColumn != "No Selection") s", facet_col='$facetColumn'" else ""
    val plotFunc = if (plotType == "area") "px.area" else "px.line"
    val showSlider = if (showRangeSlider) "True" else "False"

    s"""
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
       |            table['$timeColumn'] = pd.to_datetime(table['$timeColumn'], errors='coerce')
       |            table = table.dropna(subset=$dropnaStr).sort_values(by='$timeColumn')
       |
       |            if table.empty:
       |                yield {'html-content': self.render_error("Table became empty after filtering.")}
       |                return
       |
       |            fig = $plotFunc(table, x='$timeColumn', y='$valueColumn'$colorArg$facetArg)
       |
       |            if $showSlider:
       |                fig.update_xaxes(rangeslider_visible=True)
       |
       |            fig.update_layout(
       |                margin=dict(l=0, r=0, t=30, b=0),
       |                title=dict(text="Time Series Plot", x=0.5),
       |                xaxis_title="$timeColumn",
       |                yaxis_title="$valueColumn",
       |                template="plotly_white"
       |            )
       |
       |            html = plotly.io.to_html(fig, include_plotlyjs='cdn', full_html=False)
       |            yield {'html-content': html}
       |
       |        except Exception as e:
       |            yield {'html-content': self.render_error(str(e))}
       |""".stripMargin
  }
}
