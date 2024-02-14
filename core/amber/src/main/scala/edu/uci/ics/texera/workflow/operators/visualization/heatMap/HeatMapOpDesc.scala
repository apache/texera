package edu.uci.ics.texera.workflow.operators.visualization.heatMap

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

class HeatMapOpDesc extends VisualizationOperator with PythonOperatorDescriptor {
  @JsonProperty(defaultValue = "HeatMap", required = true)
  @JsonSchemaTitle("Title")
  @JsonPropertyDescription("Add a title to your visualization")
  var title: String = ""

  @JsonProperty(value = "x", required = true)
  @JsonSchemaTitle("Value X Column")
  @JsonPropertyDescription("the values along the x-axis")
  @AutofillAttributeName
  var x: String = ""

  @JsonProperty(value = "y", required = true)
  @JsonSchemaTitle("Value Y Column")
  @JsonPropertyDescription("the values along the y-axis")
  @AutofillAttributeName
  var y: String = ""

  @JsonProperty(value = "Values", required = true)
  @JsonSchemaTitle("Values")
  @JsonPropertyDescription("the values of the heatmap")
  @AutofillAttributeName
  var value: String = ""

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "HeatMap Chart",
      "Visualize data in a HeatMap Chart",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  private def createHeatMap(): String = {
    assert(x.nonEmpty)
    assert(y.nonEmpty)
    assert(value.nonEmpty)
    s"""
       |        heatmap = go.Heatmap(z=table["$value"],x=table["$x"],y=table["$y"])
       |        layout = go.Layout(title='$title')
       |        fig = go.Figure(data=[heatmap], layout=layout)
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalcode =
      s"""
         |from pytexera import *
         |
         |import plotly.express as px
         |import plotly.graph_objects as go
         |import plotly.io
         |import pandas as pd
         |import numpy as np
         |
         |class ProcessTableOperator(UDFTableOperator):
         |    def render_error(self, error_msg):
         |        return '''<h1>Chart is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty.")}
         |           return
         |        ${createHeatMap()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |
         |""".stripMargin
    finalcode
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
