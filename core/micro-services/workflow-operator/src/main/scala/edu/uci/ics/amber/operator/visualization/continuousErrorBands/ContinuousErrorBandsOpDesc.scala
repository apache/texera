package edu.uci.ics.amber.operator.visualization.continuousErrorBands

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.OperatorInfo
import edu.uci.ics.amber.operator.metadata.OperatorGroupConstants
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}
import edu.uci.ics.amber.operator.visualization.{VisualizationConstants, VisualizationOperator}

import java.util
import scala.jdk.CollectionConverters.ListHasAsScala

class ContinuousErrorBandsOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "xLabel", required = false, defaultValue = "X Axis")
  @JsonSchemaTitle("X Label")
  @JsonPropertyDescription("Label used for x axis")
  var xLabel: String = ""

  @JsonProperty(value = "yLabel", required = false, defaultValue = "Y Axis")
  @JsonSchemaTitle("Y Label")
  @JsonPropertyDescription("Label used for y axis")
  var yLabel: String = ""

  @JsonProperty(value = "bands", required = true)
  var bands: util.List[BandConfig] = _

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Continuous Error Bands",
      "Visualize error or uncertainty along a continuous line",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def createPlotlyFigure(): String = {
    val bandsPart = bands.asScala
      .map { bandConf =>
        val colorPart = if (bandConf.color != "") {
          s"line={'color':'${bandConf.color}'}, marker={'color':'${bandConf.color}'}, "
        } else {
          ""
        }

        val fillColorPart = if (bandConf.fillColor != "") {
          s"fillcolor='${bandConf.fillColor}', "
        } else {
          ""
        }

        val namePart = if (bandConf.name != "") {
          s"name='${bandConf.name}'"
        } else {
          s"name='${bandConf.yValue}'"
        }

        s"""fig.add_trace(go.Scatter(
            x=table['${bandConf.xValue}'],
            y=table['${bandConf.yUpper}'],
            mode='lines',
            marker=dict(color="#444"),
            line=dict(width=0),
            showlegend=False,
            $namePart
          ))
        fig.add_trace(go.Scatter(
            x=table['${bandConf.xValue}'],
            y=table['${bandConf.yLower}'],
            mode='lines',
            marker=dict(color="#444"),
            line=dict(width=0),
            fill='tonexty',
            showlegend=False,
            $fillColorPart
            $namePart
          ))
        fig.add_trace(go.Scatter(
            x=table['${bandConf.xValue}'],
            y=table['${bandConf.yValue}'],
            mode='${bandConf.mode.getModeInPlotly}',
            $colorPart
            $namePart
          ))"""
      }

    s"""
       |        fig = go.Figure()
       |        ${bandsPart.mkString("\n        ")}
       |        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0),
       |                          xaxis_title='$xLabel',
       |                          yaxis_title='$yLabel',
       |                          hovermode="x")
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode =
      s"""
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
         |""".stripMargin
    finalCode
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
