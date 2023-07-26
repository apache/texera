package edu.uci.ics.texera.workflow.operators.visualization.filledAreaPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
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
  @JsonSchemaTitle("Title")
  @JsonPropertyDescription("Title of our plot")
  @AutofillAttributeName
  var Title: String = ""

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
  @JsonPropertyDescription("Choose an attribute to color the plot")
  @AutofillAttributeName
  var Color: String = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Split Plot by  Line Group")
  @JsonPropertyDescription("Do you want to split the graph")
  @AutofillAttributeName
  var FacetColumn: Boolean = false

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "FilledAreaPlot",
      "Visualize data in filled area plot",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def numWorkers() = 1

  def createPlotlyFigure(): String = {
    assert(X.nonEmpty)
    assert(Y.nonEmpty)

    if (FacetColumn) {
      assert(LineGroup.nonEmpty)
    }

    if (Color.nonEmpty && FacetColumn) {
      s"""
         |            fig = None
         |            fig = px.area(table, x="$X", y="$Y", line_group="$LineGroup", color="$Color", facet_col="$LineGroup", title="$Title")
         |
         |""".stripMargin
    } else if (Color.nonEmpty) {
      s"""
         |            fig = None
         |            fig = px.area(table, x="$X", y="$Y", line_group="$LineGroup", color="$Color", title="$Title")
         |
         |""".stripMargin
    } else if (FacetColumn) {
      s"""
         |            fig = None
         |            fig = px.area(table, x="$X", y="$Y", line_group="$LineGroup", facet_col="$LineGroup", title="$Title")
         |
         |""".stripMargin
    } else if (LineGroup.nonEmpty) {
      s"""
         |            fig = None
         |            fig = px.area(table, x="$X", y="$Y", line_group="$LineGroup", title="$Title")
         |
         |""".stripMargin
    } else {
      s"""
         |            fig = None
         |            fig = px.area(table, x="$X", y="$Y", title="$Title")
         |
         |""".stripMargin
    }
  }

  def performTableCheck(): String = {
    s"""
       |        error = False
       |        if "$X" not in columns or "$Y" not in columns:
       |            error = True
       |        elif "$LineGroup" != "":
       |            grouped = table.groupby("$LineGroup")
       |            x_values = None
       |
       |            tolerance = (len(grouped) // 100) * 5
       |            count = 0
       |
       |            for _, group in grouped:
       |                if x_values == None:
       |                    x_values = set(group["$X"].unique())
       |                elif set(group["$X"].unique()).intersection(x_values):
       |                    X_values = x_values.union(set(group["$X"].unique()))
       |                elif not set(group["$X"].unique()).intersection(x_values):
       |                    count += 1
       |                    if count > tolerance:
       |                        error = True
       |
       |
       |""".stripMargin
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
         |        ${performTableCheck()}
         |
         |        if not error:
         |            ${createPlotlyFigure()}
         |
         |            html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |            yield {'html-content': html}
         |        else:
         |            html = '''<h1>Plot is not available.</h1>
         |                     <p>Possible reasons are:</p>
         |                     <ul>
         |                     <li>Attributes not existed</li>
         |                     <li>X attribute is not shared across all line groups</li>
         |                     </ul>'''
         |            yield {'html-content': html}
         |""".stripMargin
    final_code
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
