package edu.uci.ics.texera.workflow.operators.visualization.scatterplot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

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
  var xColumn: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y-Column")
  @JsonPropertyDescription("Y Column")
  var yColumn: String = ""

  override def chartType: String = VisualizationConstants.HTML_VIZ

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
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
    s"""
       |        # drops rows with missing values pertaining to relevant columns
       |        table.dropna(subset=['$xColumn', '$yColumn'], inplace = True)
       |
       |""".stripMargin
  }

  def createPlotlyFigure(): String = {
    assert(xColumn.nonEmpty && yColumn.nonEmpty)
    s"""
           |        fig = go.Figure(px.scatter(table, x='$xColumn', y='$yColumn'))
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
           |        return '''<h1>TreeMap is not available.</h1>
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
