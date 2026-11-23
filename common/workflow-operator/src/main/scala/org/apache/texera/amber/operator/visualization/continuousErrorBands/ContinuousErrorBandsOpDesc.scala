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

import org.apache.texera.amber.operator.visualization.StandaloneChartCode._
import org.apache.texera.amber.operator.visualization.PythonCodeFragment._
import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.{
  PythonTemplateBuilderStringContext,
  pyStringLiteral
}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.operator.visualization.PlotlyStandaloneCode
import org.apache.texera.amber.operator.visualization.PythonVisualizationOpDesc
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder

import java.util
import javax.validation.constraints.NotEmpty
import scala.jdk.CollectionConverters.ListHasAsScala
class ContinuousErrorBandsOpDesc extends PythonVisualizationOpDesc with PlotlyStandaloneCode {

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

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Continuous Error Bands",
      "Visualize error or uncertainty along a continuous line",
      OperatorGroupConstants.VISUALIZATION_STATISTICAL_GROUP
    )

  private def buildFigure(
      tableVar: String,
      encode: EncodableString => PythonTemplateBuilder,
      indent: Int
  ): PythonTemplateBuilder = {
    val pad = " " * indent
    val newline = "\n"
    val traces = bands.asScala
      .map { bandConf =>
        val xExpr = encode(bandConf.xValue)
        val yUpperExpr = encode(bandConf.yUpper)
        val yLowerExpr = encode(bandConf.yLower)
        val yValExpr = encode(bandConf.yValue)
        val nameExpr = encode(if (bandConf.name != "") bandConf.name else bandConf.yValue)
        val colorPart =
          if (bandConf.color != "") {
            val colorExpr = encode(bandConf.color)
            pyb"line={'color': $colorExpr}, marker={'color': $colorExpr}, "
          } else pyb""
        val fillColorPart =
          if (bandConf.fillColor != "") pyb"fillcolor=${encode(bandConf.fillColor)}, "
          else pyb""
        val mode = bandConf.mode.getModeInPlotly
        pyb"""${pad}fig.add_trace(go.Scatter(
             |${pad}    x=$tableVar[$xExpr],
             |${pad}    y=$tableVar[$yUpperExpr],
             |${pad}    mode='lines',
             |${pad}    marker=dict(color="#444"),
             |${pad}    line=dict(width=0),
             |${pad}    showlegend=False,
             |${pad}    name=$nameExpr
             |${pad}))
             |${pad}fig.add_trace(go.Scatter(
             |${pad}    x=$tableVar[$xExpr],
             |${pad}    y=$tableVar[$yLowerExpr],
             |${pad}    mode='lines',
             |${pad}    marker=dict(color="#444"),
             |${pad}    line=dict(width=0),
             |${pad}    fill='tonexty',
             |${pad}    showlegend=False,
             |${pad}    $fillColorPart
             |${pad}    name=$nameExpr
             |${pad}))
             |${pad}fig.add_trace(go.Scatter(
             |${pad}    x=$tableVar[$xExpr],
             |${pad}    y=$tableVar[$yValExpr],
             |${pad}    mode='$mode',
             |${pad}    $colorPart
             |${pad}    name=$nameExpr
             |${pad}))"""
      }
      .reduceOption(_ + pyb"$newline" + _)
      .getOrElse(pyb"")
    pyb"""${pad}fig = go.Figure()
         |$traces
         |${pad}fig.update_layout(margin=dict(t=0, b=0, l=0, r=0),
         |${pad}                  xaxis_title=${encode(xLabel)},
         |${pad}                  yaxis_title=${encode(yLabel)},
         |${pad}                  hovermode="x")"""
  }

  def createPlotlyFigure(): PythonTemplateBuilder = {
    assert(bands != null && !bands.isEmpty, "Bands cannot be empty")
    buildFigure("table", s => pyb"$s", indent = 8)
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
         |${runtimeError("Continuous Error Bands is not available.", typed = true)}
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |${emptyTableGuard("input table is empty.")}
         |${createPlotlyFigure()}
         |        # convert fig to html content
         |${plotlyHtmlYield()}
         |"""
    finalCode.encode
  }

  override def generateStandaloneCode(): String = {
    // Values typed into the UI become escaped Python literals: the runtime
    // path splices them as decode expressions, which a standalone script has
    // no decoder for, and hand-written quotes break on a value containing one.
    // The empty-input branch mirrors generatePythonCode's `if table.empty` guard.
    s"""${standaloneError("Continuous Error Bands is not available.")}
       |
       |if in1df.empty:
       |${standaloneErrorPage("input table is empty.")}
       |else:
       |${buildFigure("in1df", s => { val lit: String = pyStringLiteral(s); pyb"$lit" }, indent = 4).plain}
       |${standaloneFigure("Continuous error bands", indent = 4, bothOutputs = true)}""".stripMargin
  }
}
