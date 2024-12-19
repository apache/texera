package edu.uci.ics.amber.operator.visualization.contourPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}

class ContourPlotOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "x", required = true)
  @JsonSchemaTitle("x")
  @JsonPropertyDescription("The column name of X-axis")
  @AutofillAttributeName
  var x: String = ""

  @JsonProperty(value = "y", required = true)
  @JsonSchemaTitle("y")
  @JsonPropertyDescription("The column name of Y-axis")
  @AutofillAttributeName
  var y: String = ""

  @JsonProperty(value = "z", required = true)
  @JsonSchemaTitle("z")
  @JsonPropertyDescription("The column name of color bar")
  @AutofillAttributeName
  var z: String = ""

  @JsonProperty(required = false, defaultValue = "10")
  @JsonSchemaTitle("Grid Size")
  @JsonPropertyDescription("Grid resolution of the final image")
  var gridSize: String = ""

  @JsonProperty(required = false, defaultValue = "true")
  @JsonSchemaTitle("Connect Gaps")
  @JsonPropertyDescription("Automatically fill in the missing parts")
  var connectGaps: Boolean = Boolean.box(false)

  @JsonProperty(
    value = "Coloring Method",
    required = false,
    defaultValue = "heatmap"
  )
  var coloringMethod: ContourPlotColoringFunction = _

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Contour Plot",
      "Displays terrain or gradient variations in a Contour Plot",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  override def generatePythonCode(): String = {
    s"""from pytexera import *
       |import numpy as np
       |import plotly.graph_objects as go
       |from scipy.interpolate import griddata
       |import plotly.io as pio
       |
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        x = table['$x'].values
       |        y = table['$y'].values
       |        z = table['$z'].values
       |        grid_size = int('$gridSize')
       |        connGaps = True if '$connectGaps' == 'true' else False
       |
       |        grid_x, grid_y = np.meshgrid(np.linspace(min(x), max(x), grid_size), np.linspace(min(y), max(y), grid_size))
       |        grid_z = griddata((x, y), z, (grid_x, grid_y), method='cubic')
       |
       |        fig = go.Figure(data=go.Contour(
       |            x=np.linspace(min(x), max(x), grid_size),
       |            y=np.linspace(min(y), max(y), grid_size),
       |            z=grid_z,
       |            connectgaps=connGaps,
       |            contours_coloring ='${coloringMethod.getColoringMethod}',
       |            colorbar_title='$z'
       |        ))
       |        fig.update_layout(title='Contour Plot')
       |        html = pio.to_html(fig, include_plotlyjs='cdn', full_html=False)
       |        yield {'html-content': html}
       |""".stripMargin
  }
}
