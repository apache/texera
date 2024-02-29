package edu.uci.ics.texera.workflow.operators.visualization.funnelPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "title": "string"
  }
}
""")
class FunnelPlotOpDesc extends VisualizationOperator with PythonOperatorDescriptor {
  @JsonProperty(value = "title", required = true, defaultValue = "Funnel Plot")
  @JsonSchemaTitle("Plot Title")
  @JsonPropertyDescription("The value for the plot title")
  var title: String = "Funnel Plot"

  @JsonProperty(required = true)
  @JsonSchemaTitle("X Column")
  @JsonPropertyDescription("Data column for the x-axis")
  @AutofillAttributeName
  var x: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y Column")
  @JsonPropertyDescription("Data column for the y-axis")
  @AutofillAttributeName
  var y: String = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Color Column")
  @JsonPropertyDescription("Column to categorically colorize funnel sections")
  @AutofillAttributeName
  var color: String = ""

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "FunnelPlot",
      "Visualize data in a Funnel Plot",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  private def createPlotlyFigure(): String = {
    assert(x.nonEmpty)
    assert(y.nonEmpty)
    val colorArg = if (color.nonEmpty) s""", color="$color"""" else ""
    s"""
       |        fig = go.Figure(px.funnel(table, x ="$x", y = "$y"$colorArg))
       |        fig.update_layout(
       |            title='$title',
       |            scene=dict(
       |                xaxis_title='X: $x',
       |                yaxis_title='Y: $y',
       |            ),
       |            margin=dict(t=40, b=30, l=10, r=10)
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

  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
