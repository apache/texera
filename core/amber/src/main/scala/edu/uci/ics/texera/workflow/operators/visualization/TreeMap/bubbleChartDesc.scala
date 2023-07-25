package edu.uci.ics.texera.workflow.operators.visualization.TreeMap

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.annotations.{AutofillAttributeName, AutofillAttributeNameList}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.aggregate.{AggregationFunction, AggregationOperation, SpecializedAggregateOpDesc}
import edu.uci.ics.texera.workflow.operators.visualization.{VisualizationConstants, VisualizationOperator}

import java.util.Collections.singletonList
import scala.jdk.CollectionConverters.asScalaBuffer

/**
 * Visualization Operator to visualize results as a Bubble Chart
 * User specifies 2 columns to use for the x, y labels. Size of bubbles determined via
 * third column of data.
 * NOTE: This is for the PR for Texera Summer Research 2023 -VT
 */

// type can be numerical only
class bubbleChartDesc extends VisualizationOperator with PythonOperatorDescriptor {
  @JsonProperty(value = "x_value", required = true)
  @JsonSchemaTitle("X-Column")
  @JsonPropertyDescription("column of data for the x-axis")
  @AutofillAttributeName var x_value: String = ""

  @JsonProperty(value = "y_value", required = true)
  @JsonSchemaTitle("Y-Column")
  @JsonPropertyDescription("column of data for the y-axis")
  @AutofillAttributeName var y_value: String = ""

  @JsonProperty(value = "z_value", required = true)
  @JsonSchemaTitle("Z-Column")
  @JsonPropertyDescription("column of data to determine size of bubbles")
  @AutofillAttributeName var z_value: String = ""

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
       |        # replaces missing values with 0
       |        table = table.dropna(0)
       |
       |""".stripMargin
  }

  // creates the plotly figure via python code
  def createPlotlyFigure(): String = {
    assert(x_value.nonEmpty && y_value.nonEmpty && z_value.nonEmpty)
    s"""
       |        default_size = 10 # in case values are missing
       |        fig = go.Figure(px.scatter(table, x='$x_value', y='$y_value', size='$z_value', size_max = default_size))
       |
       |""".stripMargin
  }

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String
  = {
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
        |    @overrides
        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        |        ${manipulateTable()}
        |        ${createPlotlyFigure()}
        |        html = plotly.io.to_html(fig, include_plotlyjs = 'cdn', auto_play = False)
        |        # use latest plotly lib in html
        |        # html = html.replace('https://cdn.plot.ly/plotly-2.3.1.min.js', 'https://cdn.plot.ly/plotly-2.18.2.min.js')
        |        yield {'html-content':html}
        |""".stripMargin
      final_code
  }
}
