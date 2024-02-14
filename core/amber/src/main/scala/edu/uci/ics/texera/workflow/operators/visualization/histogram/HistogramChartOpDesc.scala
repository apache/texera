package edu.uci.ics.texera.workflow.operators.visualization.histogram

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

class HistogramChartOpDesc extends VisualizationOperator with PythonOperatorDescriptor {
  @JsonProperty(required = false, defaultValue = "Histogram Graph Visual")
  @JsonSchemaTitle("Title")
  @JsonPropertyDescription("Add a title to your histogram graph")
  var title: String = "Histogram Visualization"

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("Choose a column to analyze")
  @AutofillAttributeName
  var value: String = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Category")
  @JsonPropertyDescription("Choose a column for assigning different colors based on the value")
  @AutofillAttributeName
  var category: String = ""

  @JsonProperty(defaultValue = "false", required = false)
  @JsonSchemaTitle("Separate Graphs")
  @JsonPropertyDescription("Separate graphs for each category")
  var separate: Boolean = _

  /**
    * This method is to be implemented to generate the actual Python source code
    * based on operators predicates.
    *
    * @return a String representation of the executable Python source code.
    */

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Histogram",
      "Visualize data in a Histogram",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def createPlotlyFigure(): String = {
    assert(value.nonEmpty)
    assert(category.nonEmpty)
    var truthy = ""
    if (separate) truthy = "True"
    s"""
       |        if ($truthy):
       |            fig = px.histogram(table, x = '$value', title = '$title', color = '$category', text_auto = True, facet_col = '$category')
       |        else:
       |            fig = px.histogram(table, x = '$value', title = '$title', color = '$category', text_auto = True)
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
         |    def render_error(self, error_msg):
         |        return '''<h1>PieChart is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        original_table = table
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty.")}
         |           return
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |
         |""".stripMargin
    finalCode
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
