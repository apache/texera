package edu.uci.ics.texera.workflow.operators.visualization.lineChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

import java.util
import scala.jdk.CollectionConverters.ListHasAsScala

class LineChartOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "title", required = false, defaultValue = "Line Chart")
  @JsonSchemaTitle("Plot Title")
  @JsonPropertyDescription("The value for the plot tile")
  var title: String = ""

  @JsonProperty(value = "yLabel", required = false, defaultValue = "Y Axis")
  @JsonSchemaTitle("Y Label")
  @JsonPropertyDescription("the label for y axis")
  var yLabel: String = ""

  @JsonProperty(value = "xLabel", required = false, defaultValue = "X Axis")
  @JsonSchemaTitle("X Label")
  @JsonPropertyDescription("the label for x axis")
  var xLabel: String = ""

  @JsonProperty(value = "lines", required = true)
  var lines: util.List[LineConfig] = _

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Line Chart",
      "View the result in line chart",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def createPlotlyFigure(): String = {
    val linesPart = lines.asScala
      .map { lineConf =>
        val colorPart = if (lineConf.color != "") {
          s"line={'color':'${lineConf.color}'}, marker={'color':'${lineConf.color}'}, "
        } else {
          ""
        }

        val namePart = if (lineConf.name != "") {
          s"name='${lineConf.name}'"
        } else {
          s"name='${lineConf.yValue}'"
        }

        s"""fig.add_trace(go.Scatter(
            x=table['${lineConf.xValue}'],
            y=table['${lineConf.yValue}'],
            mode='${lineConf.mode.getModeInPlotly}',
            $colorPart
            $namePart
          ))"""
      }

    s"""
       |        fig = go.Figure()
       |        ${linesPart.mkString("\n        ")}
       |        fig.update_layout(title='$title',
       |                   xaxis_title='$xLabel',
       |                   yaxis_title='$yLabel')
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode =
      s"""
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
         |""".stripMargin
    finalCode
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
