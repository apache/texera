package edu.uci.ics.amber.operator.visualization.lineChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}

import java.util
import scala.jdk.CollectionConverters.ListHasAsScala

class LineChartOpDesc extends PythonOperatorDescriptor {

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
      "Line Chart",
      "View the result in line chart",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
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
       |        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0),
       |                          xaxis_title='$xLabel',
       |                          yaxis_title='$yLabel')
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

}
