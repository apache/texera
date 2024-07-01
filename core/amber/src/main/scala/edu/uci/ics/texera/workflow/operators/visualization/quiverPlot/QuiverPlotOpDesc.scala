package edu.uci.ics.texera.workflow.operators.visualization.quiverPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "value": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class QuiverPlotOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "x", required = true)
  @JsonSchemaTitle("x")
  @JsonPropertyDescription("column for the x coordinate")
  @AutofillAttributeName var x: String = ""

  @JsonProperty(value = "y", required = true)
  @JsonSchemaTitle("y")
  @JsonPropertyDescription("column for the y coordinate")
  @AutofillAttributeName var y: String = ""

  @JsonProperty(value = "u", required = true)
  @JsonSchemaTitle("u")
  @JsonPropertyDescription("column for the u component")
  @AutofillAttributeName var u: String = ""

  @JsonProperty(value = "v", required = true)
  @JsonSchemaTitle("v")
  @JsonPropertyDescription("column for the v component")
  @AutofillAttributeName var v: String = ""

  @JsonProperty(value = "pointx", required = false)
  @JsonSchemaTitle("x coordinate")
  @JsonPropertyDescription("x coordinate of point")
  var pointX: java.lang.Double = null

  @JsonProperty(value = "pointy", required = false)
  @JsonSchemaTitle("y coordinate")
  @JsonPropertyDescription("y coordinate of point")
  var pointY: java.lang.Double = null

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Quiver Plot",
      "Visualize vector data in a Quiver Plot",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def manipulateTable(): String = {
    s"""
       |        table = table.dropna() #remove missing values
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val pointXStr = if (pointX != null) pointX.toString else "None"
    val pointYStr = if (pointY != null) pointY.toString else "None"

    val finalCode = s"""
                       |from pytexera import *
                       |import pandas as pd
                       |import plotly.figure_factory as ff
                       |import numpy as np
                       |import plotly.io
                       |import plotly.graph_objects as go
                       |
                       |class ProcessTableOperator(UDFTableOperator):
                       |
                       |    def render_error(self, error_msg):
                       |        return '''<h1>Quiver Plot is not available.</h1>
                       |                  <p>Reasons are: {} </p>
                       |               '''.format(error_msg)
                       |
                       |    @overrides
                       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
                       |        if table.empty:
                       |            yield {'html-content': self.render_error("Input table is empty.")}
                       |            return
                       |
                       |        required_columns = {'${x}', '${y}', '${u}', '${v}'}
                       |        if not required_columns.issubset(table.columns):
                       |            yield {'html-content': self.render_error(f"Input table must contain columns: {', '.join(required_columns)}")}
                       |            return
                       |
                       |        ${manipulateTable()}
                       |
                       |        try:
                       |            #graph the quiver plot
                       |            fig = ff.create_quiver(
                       |                table['x'], table['y'],
                       |                table['u'], table['v'],
                       |                scale=0.1
                       |            )
                       |            #add the point into the quiver plot if the point exist
                       |            if $pointXStr != "None" and $pointYStr != "None":
                       |                       fig.add_trace(go.Scatter(x=[$pointXStr], y=[$pointYStr],
                       |                       mode='markers',
                       |                       marker=dict(size=12),
                       |                       name='point'))
                       |            html = fig.to_html(include_plotlyjs='cdn', full_html=False)
                       |        except Exception as e:
                       |            yield {'html-content': self.render_error(f"Plotly error: {str(e)}")}
                       |            return
                       |
                       |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
                       |        yield {'html-content': html}
                       |""".stripMargin
    finalCode
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
