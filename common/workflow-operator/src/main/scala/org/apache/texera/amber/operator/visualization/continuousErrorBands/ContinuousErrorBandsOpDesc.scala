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
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{
  PythonTemplateBuilderStringContext,
  pyStringLiteral
}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{PythonOperatorDescriptor, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder

import java.util
import javax.validation.constraints.NotEmpty
import scala.jdk.CollectionConverters.ListHasAsScala
class ContinuousErrorBandsOpDesc extends PythonOperatorDescriptor with StandaloneCodeGenerator {

  @JsonProperty(value = "xLabel", required = false, defaultValue = "X Axis")
  @JsonSchemaTitle("X Label")
  @JsonPropertyDescription("Label used for x axis")
  var xLabel: EncodableString = ""

  @JsonProperty(value = "yLabel", required = false, defaultValue = "Y Axis")
  @JsonSchemaTitle("Y Label")
  @JsonPropertyDescription("Label used for y axis")
  var yLabel: EncodableString = ""

  @JsonProperty(value = "bands", required = true)
  @NotEmpty(message = "Bands cannot be empty")
  var bands: util.List[BandConfig] = new util.ArrayList[BandConfig]()

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Continuous Error Bands",
      "Visualize error or uncertainty along a continuous line",
      OperatorGroupConstants.VISUALIZATION_STATISTICAL_GROUP
    )

  def createPlotlyFigure(): PythonTemplateBuilder = {
    assert(bands != null && !bands.isEmpty, "Bands cannot be empty")
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
            mode='${bandConf.mode.getModeInPlotly}',
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

  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {
    val traces =
      bands.asScala
        .map { bandConf =>
          // Values typed into the UI become escaped Python literals: the runtime
          // path splices them as decode expressions, which a standalone script has
          // no decoder for, and hand-written quotes break on a value containing one.
          val colorLit = pyStringLiteral(bandConf.color)
          val colorPart =
            if (bandConf.color != "")
              s"""line={'color': $colorLit}, marker={'color': $colorLit}, """
            else ""
          val fillColorPart =
            if (bandConf.fillColor != "")
              s"""fillcolor=${pyStringLiteral(bandConf.fillColor)}, """
            else ""
          val nameLit =
            pyStringLiteral(if (bandConf.name != "") bandConf.name else bandConf.yValue)
          val xLit = pyStringLiteral(bandConf.xValue)

          s"""fig.add_trace(go.Scatter(
             |    x=in1df[$xLit],
             |    y=in1df[${pyStringLiteral(bandConf.yUpper)}],
             |    mode='lines',
             |    marker=dict(color="#444"),
             |    line=dict(width=0),
             |    showlegend=False,
             |    name=$nameLit
             |))
             |fig.add_trace(go.Scatter(
             |    x=in1df[$xLit],
             |    y=in1df[${pyStringLiteral(bandConf.yLower)}],
             |    mode='lines',
             |    marker=dict(color="#444"),
             |    line=dict(width=0),
             |    fill='tonexty',
             |    showlegend=False,
             |    $fillColorPart
             |    name=$nameLit
             |))
             |fig.add_trace(go.Scatter(
             |    x=in1df[$xLit],
             |    y=in1df[${pyStringLiteral(bandConf.yValue)}],
             |    mode='${bandConf.mode.getModeInPlotly}',
             |    $colorPart
             |    name=$nameLit
             |))""".stripMargin
        }
        .mkString("\n")

    // Nested under the `else` below, so every trace line needs the extra level.
    val indentedTraces =
      traces.linesIterator.map(line => if (line.isEmpty) line else s"    $line").mkString("\n")

    // The empty-input branch mirrors generatePythonCode's `if table.empty`
    // guard: without it the traces index columns of a frame that has none, and
    // an empty input silently renders a blank chart instead of saying why.
    s"""def render_error(error_msg):
       |    return '''<h1>Continuous Error Bands is not available.</h1>
       |              <p>Reason is: {} </p>
       |           '''.format(error_msg)
       |
       |if in1df.empty:
       |    with open("output.html", "w", encoding="utf-8") as output:
       |        output.write(render_error("input table is empty."))
       |else:
       |    fig = go.Figure()
       |$indentedTraces
       |    fig.update_layout(margin=dict(t=0, b=0, l=0, r=0),
       |                      xaxis_title=${pyStringLiteral(xLabel)},
       |                      yaxis_title=${pyStringLiteral(yLabel)},
       |                      hovermode="x")
       |    fig.write_json("output.json")
       |    fig.write_html("output.html")
       |    print("Continuous error bands saved to output.json and output.html")""".stripMargin
  }
}
