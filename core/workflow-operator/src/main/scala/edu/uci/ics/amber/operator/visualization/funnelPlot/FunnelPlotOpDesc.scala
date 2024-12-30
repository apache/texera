package edu.uci.ics.amber.operator.visualization.funnelPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort}
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "title": "string"
  }
}
""")
class FunnelPlotOpDesc extends PythonOperatorDescriptor {

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
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  private def createPlotlyFigure(): String = {
    assert(x.nonEmpty)
    assert(y.nonEmpty)
    val colorArg = if (color.nonEmpty) s""", color="$color"""" else ""
    s"""
       |        fig = go.Figure(px.funnel(table, x ="$x", y = "$y"$colorArg))
       |        fig.update_layout(
       |            scene=dict(
       |                xaxis_title='X: $x',
       |                yaxis_title='Y: $y',
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
}
