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

  /** The figure both paths draw. `render` writes one value typed into a setting in
    * the caller's dialect: a decode expression for the operator, an escaped literal
    * for the exported script.
    *
    * Plain `s` text rather than `pyb`, so that a value spliced in without `render`
    * reaches both paths raw, where PythonCodeRawInvalidTextSpec catches it in the
    * operator. Under `pyb` it would still be encoded in the operator and pass that
    * check, but reach the exported script unquoted.
    */
  private def buildFigure(
      tableVar: String,
      render: EncodableString => String,
      indent: Int
  ): String = {
    val pad = " " * indent
    val traces = bands.asScala
      .map { bandConf =>
        val x = render(bandConf.xValue)
        val name = render(if (bandConf.name != "") bandConf.name else bandConf.yValue)
        val colorPart =
          if (bandConf.color != "") {
            val color = render(bandConf.color)
            s"line={'color': $color}, marker={'color': $color}, "
          } else ""
        val fillColorPart =
          if (bandConf.fillColor != "") s"fillcolor=${render(bandConf.fillColor)}, " else ""
        s"""${pad}fig.add_trace(go.Scatter(
           |${pad}    x=$tableVar[$x],
           |${pad}    y=$tableVar[${render(bandConf.yUpper)}],
           |${pad}    mode='lines',
           |${pad}    marker=dict(color="#444"),
           |${pad}    line=dict(width=0),
           |${pad}    showlegend=False,
           |${pad}    name=$name
           |${pad}))
           |${pad}fig.add_trace(go.Scatter(
           |${pad}    x=$tableVar[$x],
           |${pad}    y=$tableVar[${render(bandConf.yLower)}],
           |${pad}    mode='lines',
           |${pad}    marker=dict(color="#444"),
           |${pad}    line=dict(width=0),
           |${pad}    fill='tonexty',
           |${pad}    showlegend=False,
           |${pad}    $fillColorPart
           |${pad}    name=$name
           |${pad}))
           |${pad}fig.add_trace(go.Scatter(
           |${pad}    x=$tableVar[$x],
           |${pad}    y=$tableVar[${render(bandConf.yValue)}],
           |${pad}    mode='${bandConf.mode.getModeInPlotly}',
           |${pad}    $colorPart
           |${pad}    name=$name
           |${pad}))""".stripMargin
      }
      .mkString("\n")
    s"""${pad}fig = go.Figure()
       |$traces
       |${pad}fig.update_layout(margin=dict(t=0, b=0, l=0, r=0),
       |${pad}                  xaxis_title=${render(xLabel)},
       |${pad}                  yaxis_title=${render(yLabel)},
       |${pad}                  hovermode="x")""".stripMargin
  }

  // Rendered in the operator's dialect up front, so `.plain` shows the decode
  // expressions as well.
  def createPlotlyFigure(): PythonTemplateBuilder = {
    assert(bands != null && !bands.isEmpty, "Bands cannot be empty")
    val figure = buildFigure("table", value => pyb"$value".encode, indent = 8)
    pyb"$figure"
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
       |${buildFigure("in1df", value => pyStringLiteral(value), indent = 4)}
       |${standaloneFigure("Continuous error bands", indent = 4, bothOutputs = true)}""".stripMargin
  }
}
