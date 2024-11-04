package edu.uci.ics.amber.operator.visualization.scatterplot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.common.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}
import edu.uci.ics.amber.operator.visualization.{VisualizationConstants, VisualizationOperator}

@JsonSchemaInject(
  json =
    "{" +
      "  \"attributeTypeRules\": {" +
      "    \"xColumn\":{" +
      "      \"enum\": [\"integer\", \"double\"]" +
      "    }," +
      "    \"yColumn\":{" +
      "      \"enum\": [\"integer\", \"double\"]" +
      "    }" +
      "  }" +
      "}"
)
class ScatterplotOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("X-Column")
  @JsonPropertyDescription("X Column")
  @AutofillAttributeName
  private val xColumn: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y-Column")
  @JsonPropertyDescription("Y Column")
  @AutofillAttributeName
  private val yColumn: String = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Color-Column")
  @JsonPropertyDescription(
    "Dots will be assigned different colors based on their values of this column"
  )
  @AutofillAttributeName
  private val colorColumn: String = ""

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("log scale X")
  @JsonPropertyDescription("values in X-column is log-scaled")
  var xLogScale: Boolean = false

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("log scale Y")
  @JsonPropertyDescription("values in Y-column is log-scaled")
  var yLogScale: Boolean = false

  @JsonProperty(required = false)
  @JsonSchemaTitle("Hover column")
  @JsonPropertyDescription("column value to display when a dot is hovered over")
  @AutofillAttributeName
  var hoverName: String = ""

  override def chartType: String = VisualizationConstants.HTML_VIZ

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Scatterplot",
      "View the result in a scatterplot",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def manipulateTable(): String = {
    assert(xColumn.nonEmpty && yColumn.nonEmpty)
    val colorColExpr = if (colorColumn.nonEmpty) { s"'$colorColumn'" }
    else { "" }
    s"""
       |        # drops rows with missing values pertaining to relevant columns
       |        table.dropna(subset=['$xColumn', '$yColumn', $colorColExpr], inplace = True)
       |
       |""".stripMargin
  }

  def createPlotlyFigure(): String = {
    assert(xColumn.nonEmpty && yColumn.nonEmpty)
    val colorColExpr = if (colorColumn.nonEmpty) { s"color='$colorColumn'" }
    else { "" }
    var argDetails = ""
    if (xLogScale) argDetails = argDetails + ", log_x=True"
    if (yLogScale) argDetails = argDetails + ", log_y=True"
    if (hoverName.nonEmpty) argDetails = argDetails + s""", hover_name='$hoverName'"""
    s"""
           |        fig = go.Figure(px.scatter(table, x='$xColumn', y='$yColumn', $colorColExpr $argDetails))
           |        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
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
           |
           |class ProcessTableOperator(UDFTableOperator):
           |
           |    def render_error(self, error_msg):
           |        return '''<h1>Scatter Plot is not available.</h1>
           |                  <p>Reasons are: {} </p>
           |               '''.format(error_msg)
           |
           |    @overrides
           |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
           |        if table.empty:
           |            yield {'html-content': self.render_error("Input table is empty.")}
           |            return
           |        ${manipulateTable()}
           |        ${createPlotlyFigure()}
           |        if table.empty:
           |            yield {'html-content': self.render_error("No valid rows left (every row has at least 1 missing value).")}
           |            return
           |        html = plotly.io.to_html(fig, include_plotlyjs = 'cdn', auto_play = False)
           |        yield {'html-content':html}
           |""".stripMargin
    finalCode
  }
}
