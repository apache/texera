package edu.uci.ics.texera.workflow.operators.visualization.bubbleChart

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

/**
  * Visualization Operator to visualize results as a Bubble Chart
  * User specifies 2 columns to use for the x, y labels. Size of bubbles determined via
  * third column of data.
  * NOTE: This is for the PR for Texera Summer Research 2023 -VT
  */

// type can be numerical only
class BubbleChartOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "x_value", required = true)
  @JsonSchemaTitle("X-Column")
  @JsonPropertyDescription("Data column for the x-axis")
  @AutofillAttributeName var x_value: String = ""

  @JsonProperty(value = "y_value", required = true)
  @JsonSchemaTitle("Y-Column")
  @JsonPropertyDescription("Data column for the y-axis")
  @AutofillAttributeName var y_value: String = ""

  @JsonProperty(value = "z_value", required = true)
  @JsonSchemaTitle("Z-Column")
  @JsonPropertyDescription("Data column to determine bubble size")
  @AutofillAttributeName var z_value: String = ""

  @JsonProperty(value = "title", required = true)
  @JsonSchemaTitle("Title")
  @JsonPropertyDescription("Title of Chart")
  var title: String = "My Bubble Chart"

  @JsonProperty(value = "enable_color", defaultValue = "false")
  @JsonSchemaTitle("Enable Color")
  @JsonPropertyDescription("Colors bubbles using a data column")
  var enable_color: Boolean = false

  @JsonProperty(value = "color_category")
  @JsonSchemaTitle("Color-Column")
  @JsonPropertyDescription("Picks data column to color bubbles with if color is enabled")
  @AutofillAttributeName var color_category: String = ""

  override def chartType: String = VisualizationConstants.HTML_VIZ

  // placeholder until I figure out how this thing works
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Bubble Chart",
      "a 3D Scatter Plot; Bubbles are graphed using x and y labels, and their sizes determined by a z-value.",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def numWorkers() = 1

  def manipulateTable(): String = {
    assert(x_value.nonEmpty && y_value.nonEmpty && z_value.nonEmpty)
    s"""
       |        # drops rows with missing values
       |        table = table.dropna()
       |
       |""".stripMargin
  }

  // creates the plotly figure via python code
  def createPlotlyFigure(): String = {
    assert(x_value.nonEmpty && y_value.nonEmpty && z_value.nonEmpty)
    s"""
       |        if '$enable_color':
       |            fig = go.Figure(px.scatter(table, x='$x_value', y='$y_value', size='$z_value', size_max=100, title='$title', color='$color_category'))
       |        else:
       |            fig = go.Figure(px.scatter(table, x='$x_value', y='$y_value', size='$z_value', size_max=100, title='$title'))
       |""".stripMargin
  }

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String = {
    val final_code = s"""
        |from pytexera import *
        |
        |import plotly.express as px
        |import pandas as pd
        |import plotly.graph_objects as go
        |import plotly.io
        |import json
        |import pickle
        |import plotly
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
    final_code
  }
}
