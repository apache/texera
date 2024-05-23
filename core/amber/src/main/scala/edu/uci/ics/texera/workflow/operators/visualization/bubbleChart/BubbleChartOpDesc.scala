package edu.uci.ics.texera.workflow.operators.visualization.bubbleChart

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

/**
  * Visualization Operator to visualize results as a Bubble Chart
  * User specifies 2 columns to use for the x, y labels. Size of bubbles determined via
  * third column of data. Bubbles can be sorted via color using a fourth column.
  */

// type can be numerical only
class BubbleChartOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "xValue", required = true)
  @JsonSchemaTitle("X-Column")
  @JsonPropertyDescription("Data column for the x-axis")
  @AutofillAttributeName var xValue: String = ""

  @JsonProperty(value = "yValue", required = true)
  @JsonSchemaTitle("Y-Column")
  @JsonPropertyDescription("Data column for the y-axis")
  @AutofillAttributeName var yValue: String = ""

  @JsonProperty(value = "zValue", required = true)
  @JsonSchemaTitle("Z-Column")
  @JsonPropertyDescription("Data column to determine bubble size")
  @AutofillAttributeName var zValue: String = ""

  @JsonProperty(value = "enableColor", defaultValue = "false")
  @JsonSchemaTitle("Enable Color")
  @JsonPropertyDescription("Colors bubbles using a data column")
  var enableColor: Boolean = false

  @JsonProperty(value = "colorCategory", required = true)
  @JsonSchemaTitle("Color-Column")
  @JsonPropertyDescription("Picks data column to color bubbles with if color is enabled")
  @AutofillAttributeName var colorCategory: String = ""

  override def chartType: String = VisualizationConstants.HTML_VIZ

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Bubble Chart",
      "a 3D Scatter Plot; Bubbles are graphed using x and y labels, and their sizes determined by a z-value.",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def manipulateTable(): String = {
    assert(xValue.nonEmpty && yValue.nonEmpty && zValue.nonEmpty)
    s"""
       |        # drops rows with missing values pertaining to relevant columns
       |        table.dropna(subset=['$xValue', '$yValue', '$zValue'], inplace = True)
       |
       |""".stripMargin
  }

  def createPlotlyFigure(): String = {
    assert(xValue.nonEmpty && yValue.nonEmpty && zValue.nonEmpty)
    s"""
       |        if '$enableColor' == 'true':
       |            fig = go.Figure(px.scatter(table, x='$xValue', y='$yValue', size='$zValue', size_max=100, color='$colorCategory'))
       |        else:
       |            fig = go.Figure(px.scatter(table, x='$xValue', y='$yValue', size='$zValue', size_max=100))
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode = s"""
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
        |        fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
        |        html = plotly.io.to_html(fig, include_plotlyjs = 'cdn', auto_play = False)
        |        yield {'html-content':html}
        |""".stripMargin
    finalCode
  }
}
