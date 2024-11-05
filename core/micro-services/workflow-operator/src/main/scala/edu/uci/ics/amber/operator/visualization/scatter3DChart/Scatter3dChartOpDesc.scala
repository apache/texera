package edu.uci.ics.amber.operator.visualization.scatter3DChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.OperatorInfo
import edu.uci.ics.amber.operator.metadata.OperatorGroupConstants
import edu.uci.ics.amber.operator.metadata.annotation.AutofillAttributeName
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}
import edu.uci.ics.amber.operator.visualization.{VisualizationConstants, VisualizationOperator}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "title": "string"
  }
}
""")
class Scatter3dChartOpDesc extends VisualizationOperator with PythonOperatorDescriptor {
  @JsonProperty(value = "x", required = true)
  @JsonSchemaTitle("X Column")
  @JsonPropertyDescription("Data column for the x-axis")
  @AutofillAttributeName
  var x: String = ""

  @JsonProperty(value = "y", required = true)
  @JsonSchemaTitle("Y Column")
  @JsonPropertyDescription("Data column for the y-axis")
  @AutofillAttributeName
  var y: String = ""

  @JsonProperty(value = "z", required = true)
  @JsonSchemaTitle("Z Column")
  @JsonPropertyDescription("Data column for the z-axis")
  @AutofillAttributeName
  var z: String = ""

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Scatter3D Chart",
      "Visualize data in a Scatter3D Plot",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  private def createPlotlyFigure(): String = {
    assert(x.nonEmpty)
    assert(y.nonEmpty)
    assert(z.nonEmpty)
    s"""
       |        fig = go.Figure(data=[go.Scatter3d(
       |            x=table["$x"],
       |            y=table["$y"],
       |            z=table["$z"],
       |            mode='markers',
       |            marker=dict(
       |                size=12,
       |                colorscale='Viridis',
       |                opacity=0.8
       |            )
       |        )])
       |        fig.update_traces(marker=dict(size=5, opacity=0.8))
       |        fig.update_layout(
       |            scene=dict(
       |                xaxis_title='X: $x',
       |                yaxis_title='Y: $y',
       |                zaxis_title='Z: $z'
       |            ),
       |            margin=dict(t=0, b=0, l=0, r=0)
       |        )
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
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |
         |""".stripMargin
    finalcode
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
