package edu.uci.ics.texera.workflow.operators.visualization.DotPlot

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

class DotPlotOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "Count Attribute", required = true)
  @JsonSchemaTitle("Count Attribute")
  @JsonPropertyDescription("the attribute for the counting of the dot plot")
  @AutofillAttributeName
  var countAttribute: String = ""

  @JsonProperty(value = "Title", required = true)
  @JsonSchemaTitle("Title")
  @JsonPropertyDescription("Title for the Dot Plot Visualization")
  var title: String = ""

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "DotPlot",
      "Visualize data using a dot plot",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def createPlotlyFigure(): String = {
    s"""
       |        table = table.groupby(['$countAttribute'])['$countAttribute'].count().reset_index(name='counts')
       |        fig = px.strip(table, x='counts', y='$countAttribute', orientation='h', color='$countAttribute',
       |               color_discrete_sequence=px.colors.qualitative.Dark2)
       |
       |        fig.update_traces(marker=dict(size=12, line=dict(width=2, color='DarkSlateGrey')))
       |
       |        fig.update_layout(title='$title',
       |                          xaxis_title='Counts',
       |                          yaxis_title='$countAttribute',
       |                          yaxis=dict(showline=True, showgrid=False, showticklabels=True),
       |                          xaxis=dict(showline=True, showgrid=True, showticklabels=True),
       |                          height=800)
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode = s"""
                        |from pytexera import *
                        |
                        |import plotly.express as px
                        |import plotly.graph_objects as go
                        |import plotly.io
                        |
                        |class ProcessTableOperator(UDFTableOperator):
                        |
                        |    def render_error(self, error_msg):
                        |        return '''<h1>DotPlot is not available.</h1>
                        |                  <p>Reasons are: {} </p>
                        |               '''.format(error_msg)
                        |
                        |    @overrides
                        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
                        |        if table.empty:
                        |            yield {'html-content': self.render_error("Input table is empty.")}
                        |            return
                        |        ${createPlotlyFigure()}
                        |        if table.empty:
                        |            yield {'html-content': self.render_error("No valid rows left (every row has at least 1 missing value).")}
                        |            return
                        |        # convert fig to html content
                        |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
                        |        yield {'html-content': html}
                        |""".stripMargin
    finalCode
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
