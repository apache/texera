package edu.uci.ics.texera.workflow.operators.visualization.filledAreaPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
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


class FilledAreaPlotVisualizerOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("X-axis Attribute")
  @JsonPropertyDescription("The attribute for your x-axis")
  @AutofillAttributeName
  var X: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y-axis Attribute")
  @JsonPropertyDescription("The attribute for your y-axis")
  @AutofillAttributeName
  var Y: String = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Line Group")
  @JsonPropertyDescription("The attribute for group of each line")
  @AutofillAttributeName
  var LineGroup: String = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Color")
  @JsonPropertyDescription("Do you want to color the lines")
  @AutofillAttributeName
  var Color: Boolean = false

  @JsonProperty(required = false)
  @JsonSchemaTitle("Split Graph")
  @JsonPropertyDescription("Do you want to split the graph")
  @AutofillAttributeName
  var FacetColumn: Boolean = false

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "FilledAreaPlot Visualizer",
      "Visualize data in filled area plot/plots",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def numWorkers() = 1

  def createPlotlyFigure(): String = {
    assert(X.nonEmpty)
    assert(Y.nonEmpty)

    if (Color || FacetColumn) {
      assert(LineGroup.nonEmpty)
    }

    if (Color && FacetColumn) {
      s"""
         |            fig = None
         |            fig = px.area(table, x="$X", y="$Y", line_group="$LineGroup", color="$LineGroup", facet_col="$LineGroup")
         |
         |""".stripMargin
    } else if (Color) {
      s"""
         |            fig = None
         |            fig = px.area(table, x="$X", y="$Y", line_group="$LineGroup", color="$LineGroup")
         |
         |""".stripMargin
    } else if (FacetColumn) {
      s"""
         |            fig = None
         |            fig = px.area(table, x="$X", y="$Y", line_group="$LineGroup", facet_col="$LineGroup")
         |
         |""".stripMargin
    } else if (LineGroup.nonEmpty){
      s"""
         |            fig = None
         |            fig = px.area(table, x="$X", y="$Y", line_group="$LineGroup")
         |
         |""".stripMargin
    } else {
      s"""
         |            fig = None
         |            fig = px.area(table, x="$X", y="$Y")
         |
         |""".stripMargin
    }
  }

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String = {
    val final_code = s"""
         |from pytexera import *
         |
         |import plotly
         |import plotly.express as px
         |import plotly.graph_objects as go
         |import plotly.io
         |import numpy as np
         |import pandas as pd
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        columns = list(table.columns)
         |        if "$X" in columns and "$Y" in columns:
         |            ${createPlotlyFigure()}
         |
         |            html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |            yield {'html-content': html}
         |        else:
         |            html = '''<h1>Plot is not available.</h1>
         |                     <p>Possible reasons are:</p>
         |                     <ul>
         |                     <li>Attributes not existed</li>
         |                     </ul>'''
         |            yield {'html-content': html}
         |""".stripMargin
    final_code
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
