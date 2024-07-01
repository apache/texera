package edu.uci.ics.texera.workflow.operators.visualization.continuousErrorBands

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

class ContinuousErrorBandsOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("X-Axis")
  @JsonPropertyDescription("Attribute used for x-axis")
  @AutofillAttributeName
  var xAxis: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y-Axis")
  @JsonPropertyDescription("Attribute used for y-axis")
  @AutofillAttributeName
  var yAxis: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y-Axis Upper Bound")
  @JsonPropertyDescription("Represents upper bound error of y-values")
  @AutofillAttributeName
  var yUpper: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y-Axis Lower Bound")
  @JsonPropertyDescription("Represents lower bound error of y-values")
  @AutofillAttributeName
  var yLower: String = ""

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
    s"""
       |        fig = go.Figure([
       |          go.Scatter(
       |            name='Line 1',
       |            x=table['$xAxis'],
       |            y=table['$yAxis'],
       |            mode='lines',
       |            line=dict(color='rgb(31, 119, 180)'),
       |          ),
       |          go.Scatter(
       |            name='Upper Bound',
       |            x=table['$xAxis'],
       |            y=table['$yUpper'],
       |            mode='lines',
       |            marker=dict(color="#444"),
       |            line=dict(width=0),
       |            showlegend=False
       |          ),
       |          go.Scatter(
       |            name='Lower Bound',
       |            x=table['$xAxis'],
       |            y=table['$yLower'],
       |            marker=dict(color="#444"),
       |            line=dict(width=0),
       |            mode='lines',
       |            fillcolor='rgba(68, 68, 68, 0.3)',
       |            fill='tonexty',
       |            showlegend=False
       |          )
       |        ])
       |        fig.update_layout(
       |          yaxis_title='$yAxis',
       |          hovermode="x"
       |        )
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
