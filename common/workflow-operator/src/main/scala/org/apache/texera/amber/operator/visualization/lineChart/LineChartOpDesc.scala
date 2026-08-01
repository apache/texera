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

package org.apache.texera.amber.operator.visualization.lineChart

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

class LineChartOpDesc extends PythonOperatorDescriptor with StandaloneCodeGenerator {

  @JsonProperty(value = "yLabel", required = false, defaultValue = "Y Axis")
  @JsonSchemaTitle("Y Label")
  @JsonPropertyDescription("the label for y axis")
  var yLabel: EncodableString = ""

  @JsonProperty(value = "xLabel", required = false, defaultValue = "X Axis")
  @JsonSchemaTitle("X Label")
  @JsonPropertyDescription("the label for x axis")
  var xLabel: EncodableString = ""

  @JsonProperty(value = "lines", required = true)
  @NotEmpty(message = "At least one line must be configured")
  var lines: util.List[LineConfig] = new util.ArrayList[LineConfig]()

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema()
      .add("html-content", AttributeType.STRING)
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "Line Chart",
      "View the result in line chart",
      OperatorGroupConstants.VISUALIZATION_BASIC_GROUP
    )

  def createPlotlyFigure(): PythonTemplateBuilder = {
    assert(lines != null && !lines.isEmpty, "At least one line must be configured")
    val linesPart = lines.asScala
      .map { lineConf =>
        val colorPart = if (lineConf.color != "") {
          pyb"line={'color':${lineConf.color}}, marker={'color':${lineConf.color}}, "
        } else {
          pyb""
        }

        val namePart = if (lineConf.name != "") {
          pyb"name=${lineConf.name}"
        } else {
          pyb"name=${lineConf.yValue}"
        }

        pyb"""fig.add_trace(go.Scatter(
            x=table[${lineConf.xValue}],
            y=table[${lineConf.yValue}],
            mode='${lineConf.mode.getModeInPlotly}',
            $colorPart
            $namePart
          ))"""
      }

    pyb"""
       |        fig = go.Figure()
       |        ${linesPart.mkString("\n        ")}
       |        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0),
       |                          xaxis_title=$xLabel,
       |                          yaxis_title=$yLabel)
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
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Line chart is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("input table is empty.")}
         |            return
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |"""
    finalCode.encode
  }

  override def producesDataFrame(): Boolean = false

  override def generateStandaloneCode(): String = {
    val traces = lines.asScala
      .map { lineConf =>
        // Values typed into the UI become escaped Python literals: the runtime
        // path splices them as decode expressions, which a standalone script has
        // no decoder for, and hand-written quotes break on a value containing one.
        val colorLit = pyStringLiteral(lineConf.color)
        val colorPart =
          if (lineConf.color != "")
            s"""line={'color':$colorLit}, marker={'color':$colorLit}, """
          else ""
        val namePart =
          if (lineConf.name != "") s"""name=${pyStringLiteral(lineConf.name)}"""
          else s"""name=${pyStringLiteral(lineConf.yValue)}"""

        s"""fig.add_trace(go.Scatter(
           |    x=in1df[${pyStringLiteral(lineConf.xValue)}],
           |    y=in1df[${pyStringLiteral(lineConf.yValue)}],
           |    mode='${lineConf.mode.getModeInPlotly}',
           |    $colorPart
           |    $namePart
           |  ))""".stripMargin
      }
      .mkString("\n")
      // The traces sit inside the non-empty branch below, so shift them in.
      .linesIterator
      .map(line => if (line.isEmpty) line else s"    $line")
      .mkString("\n")

    s"""def render_error(error_msg) -> str:
       |    return '''<h1>Line chart is not available.</h1>
       |                  <p>Reason is: {} </p>
       |               '''.format(error_msg)
       |
       |if in1df.empty:
       |    with open("output.html", "w", encoding="utf-8") as output:
       |        output.write(render_error("input table is empty."))
       |else:
       |    fig = go.Figure()
       |$traces
       |    fig.update_layout(margin=dict(t=0, b=0, l=0, r=0),
       |                      xaxis_title=${pyStringLiteral(xLabel)},
       |                      yaxis_title=${pyStringLiteral(yLabel)})
       |    fig.write_json("output.json")
       |    fig.write_html("output.html")
       |    print("Line chart saved to output.json and output.html")""".stripMargin
  }

}
