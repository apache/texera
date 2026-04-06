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

package org.apache.texera.amber.operator.visualization.continuousErrorBands

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder

import java.util
import scala.jdk.CollectionConverters.ListHasAsScala
class ContinuousErrorBandsOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "xLabel", required = false, defaultValue = "X Axis")
  @JsonSchemaTitle("X Label")
  @JsonPropertyDescription("Label used for x axis")
  var xLabel: EncodableString = ""

  @JsonProperty(value = "yLabel", required = false, defaultValue = "Y Axis")
  @JsonSchemaTitle("Y Label")
  @JsonPropertyDescription("Label used for y axis")
  var yLabel: EncodableString = ""

  @JsonProperty(value = "bands", required = true)
  var bands: util.List[BandConfig] = _

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Continuous Error Bands",
      "Visualize error or uncertainty along a continuous line",
      OperatorGroupConstants.VISUALIZATION_STATISTICAL_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  def createPlotlyFigure(): PythonTemplateBuilder = {
    val bandsPart = bands.asScala
      .map { bandConf =>
        val colorPart = if (bandConf.color != "") {
          pyb"line={'color':${bandConf.color}}, marker={'color':${bandConf.color}}, "
        } else {
          ""
        }

        val fillColorPart = if (bandConf.fillColor != "") {
          pyb"fillcolor=${bandConf.fillColor}, "
        } else {
          ""
        }

        val namePart = if (bandConf.name != "") {
          pyb"name=${bandConf.name}"
        } else {
          pyb"name=${bandConf.yValue}"
        }

        pyb"""fig.add_trace(go.Scatter(
            x=table[${bandConf.xValue}],
            y=table[${bandConf.yUpper}],
            mode='lines',
            marker=dict(color="#444"),
            line=dict(width=0),
            showlegend=False,
            $namePart
          ))
        fig.add_trace(go.Scatter(
            x=table[${bandConf.xValue}],
            y=table[${bandConf.yLower}],
            mode='lines',
            marker=dict(color="#444"),
            line=dict(width=0),
            fill='tonexty',
            showlegend=False,
            $fillColorPart
            $namePart
          ))
        fig.add_trace(go.Scatter(
            x=table[${bandConf.xValue}],
            y=table[${bandConf.yValue}],
            mode=${bandConf.mode.getModeInPlotly},
            $colorPart
            $namePart
          ))"""
      }

    pyb"""
       |        fig = go.Figure()
       |        ${bandsPart.mkString("\n        ")}
       |        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0),
       |                          xaxis_title=$xLabel,
       |                          yaxis_title=$yLabel,
       |                          hovermode="x")
       |"""
  }

  override def generatePythonCode(): String = {
    val finalCode =
      pyb"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import plotly.graph_objs as go
         |import plotly.io
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Continuous Error Bands is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty.")}
         |           return
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |"""
    finalCode.encode
  }
}
