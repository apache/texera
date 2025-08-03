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

package edu.uci.ics.amber.operator.visualization.radarPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList
}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "selectedAttributes": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class RadarPlotOpDesc extends PythonOperatorDescriptor {
  @JsonProperty(value = "selectedAttributes", required = true)
  @JsonSchemaTitle("Axes")
  @JsonPropertyDescription("Numeric columns to use as radar axes")
  @AutofillAttributeNameList
  var selectedAttributes: List[String] = _

  @JsonProperty(value = "traceNameAttribute", defaultValue = "-- No Selection --", required = false)
  @JsonSchemaTitle("Trace Name Column")
  @JsonPropertyDescription("Optional - Select a column to use for naming each radar trace")
  @AutofillAttributeName
  var traceNameAttribute: String = ""

  @JsonProperty(
    value = "traceColorAttribute",
    defaultValue = "-- No Selection --",
    required = false
  )
  @JsonSchemaTitle("Trace Color Column")
  @JsonPropertyDescription(
    "Optional - Select a column to use for coloring each radar trace (note: if there are too many traces with distinct coloring values, colors may repeat)"
  )
  @AutofillAttributeName
  var traceColorAttribute: String = ""

  @JsonProperty(value = "linePattern", defaultValue = "solid", required = true)
  @JsonPropertyDescription("Pattern of the lines connecting points on the radar plot")
  var linePattern: RadarPlotLinePattern = _

  @JsonProperty(value = "maxNormalize", defaultValue = "true", required = true)
  @JsonSchemaTitle("Max Normalize")
  @JsonPropertyDescription(
    "Normalize radar plot values by scaling them relative to the maximum value on their respective axes"
  )
  var maxNormalize: Boolean = true

  @JsonProperty(value = "fillTrace", defaultValue = "true", required = true)
  @JsonSchemaTitle("Fill Trace")
  @JsonPropertyDescription("Fill the area within each radar trace")
  var fillTrace: Boolean = true

  @JsonProperty(value = "showMarkers", defaultValue = "true", required = true)
  @JsonSchemaTitle("Show Point Markers")
  @JsonPropertyDescription("Display point markers on the radar plot")
  var showMarkers: Boolean = true

  @JsonProperty(value = "showLegend", defaultValue = "true", required = false)
  @JsonSchemaTitle("Show Legend")
  @JsonPropertyDescription(
    "Display the legend (note: without the legend, you are unable to selectively hide or show traces in the plot)"
  )
  var showLegend: Boolean = true

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Radar Plot",
      "View the result in a radar plot. A radar plot displays multivariate data on multiple axes arranged in a circular layout, allowing for comparison between different entities.",
      OperatorGroupConstants.VISUALIZATION_SCIENTIFIC_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  def generateRadarPlotCode(): String = {
    def toPythonBool(value: Boolean): String = if (value) "True" else "False"

    val attrList = selectedAttributes.map(attr => s""""$attr"""").mkString(", ")
    val traceNameCol = traceNameAttribute match {
      case "" | "-- No Selection --" => "None"
      case col                       => s"'$col'"
    }
    val traceColorCol = traceColorAttribute match {
      case "" | "-- No Selection --" => "None"
      case col                       => s"'$col'"
    }

    s"""
       |        categories = [$attrList]
       |        if not categories:
       |            yield {'html-content': self.render_error("No columns selected as axes.")}
       |            return
       |
       |        trace_name_col = $traceNameCol
       |        trace_color_col = $traceColorCol
       |        line_pattern = "${linePattern.getLinePattern}"
       |        max_normalize = ${toPythonBool(maxNormalize)}
       |        fill_trace = ${toPythonBool(fillTrace)}
       |        show_markers = ${toPythonBool(showMarkers)}
       |        show_legend = ${toPythonBool(showLegend)}
       |
       |        selected_table_df = table[categories].astype(float)
       |        selected_table = selected_table_df.values
       |
       |        trace_names = (
       |            table[trace_name_col].values if trace_name_col
       |            else np.full(len(table), "", dtype=object)
       |        )
       |
       |        trace_colors = [None] * len(table)
       |        if trace_color_col:
       |            unique_vals = table[trace_color_col].unique()
       |            color_map = {val: px.colors.qualitative.Plotly[idx % len(px.colors.qualitative.Plotly)]
       |                         for idx, val in enumerate(unique_vals)}
       |            nan_color = '#000000'
       |            trace_colors = table[trace_color_col].map(color_map).fillna(nan_color).values
       |
       |        hover_texts = []
       |        for idx, row in enumerate(selected_table):
       |            name_prefix = str(trace_names[idx]) + "<br>" if trace_names[idx] else ""
       |            row_hover_texts = []
       |            for attr, value in zip(categories, row):
       |                row_hover_texts.append(name_prefix + attr + ": " + str(value))
       |            hover_texts.append(row_hover_texts)
       |
       |        if max_normalize:
       |            max_vals = selected_table_df.max().values
       |            max_vals[max_vals == 0] = 1
       |            selected_table = selected_table / max_vals
       |
       |        selected_table = np.nan_to_num(selected_table)
       |
       |        fig = go.Figure()
       |
       |        for idx, row in enumerate(selected_table):
       |            # To connect ensure all points in the radar trace are connected
       |            closed_row = row.tolist() + [row[0]]
       |            closed_categories = categories + [categories[0]]
       |            closed_hover_texts = hover_texts[idx] + [hover_texts[idx][0]]
       |
       |            fig.add_trace(go.Scatterpolar(
       |                r=closed_row,
       |                theta=closed_categories,
       |                fill='toself' if fill_trace else 'none',
       |                name=str(trace_names[idx]) if trace_names[idx] else "",
       |                text=closed_hover_texts,
       |                hoverinfo="text",
       |                mode="lines+markers" if show_markers else "lines",
       |                line=dict(dash=line_pattern, color=trace_colors[idx] if trace_colors[idx] else None),
       |                marker=dict(color=trace_colors[idx]) if trace_colors[idx] else {}
       |            ))
       |
       |        fig.update_layout(
       |            polar=dict(radialaxis=dict(visible=True)),
       |            showlegend=show_legend,
       |            width=600,
       |            height=600
       |        )
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    s"""
       |from pytexera import *
       |import numpy as np
       |import plotly.graph_objects as go
       |import plotly.express as px
       |import plotly.io
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    def render_error(self, error_msg):
       |        return '''<h1>Radar Plot is not available.</h1>
       |                  <p>Reason is: {} </p>
       |               '''.format(error_msg)
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int):
       |        if table.empty:
       |            yield {'html-content': self.render_error("Input table is empty.")}
       |            return
       |
       |        ${generateRadarPlotCode()}
       |
       |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False, config={'responsive': True})
       |        yield {'html-content': html}
       |""".stripMargin
  }
}
