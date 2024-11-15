package edu.uci.ics.amber.operator.visualization.waterfallChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.OperatorInfo
import edu.uci.ics.amber.operator.metadata.OperatorGroupConstants
import edu.uci.ics.amber.operator.metadata.annotation.AutofillAttributeName
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}
import edu.uci.ics.amber.operator.visualization.{VisualizationConstants, VisualizationOperator}

class WaterfallChartOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "xColumn", required = true)
  @JsonSchemaTitle("X Axis Values")
  @JsonPropertyDescription("The column representing categories or stages")
  @AutofillAttributeName
  var xColumn: String = _

  @JsonProperty(value = "yColumn", required = true)
  @JsonSchemaTitle("Y Axis Values")
  @JsonPropertyDescription("The column representing numeric values for each stage")
  @AutofillAttributeName
  var yColumn: String = _

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Waterfall Chart",
      "Visualize data as a waterfall chart",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def createPlotlyFigure(): String = {
    s"""
       |        x_values = table['$xColumn']
       |        y_values = table['$yColumn']
       |
       |        fig = go.Figure(go.Waterfall(
       |            name="Waterfall", orientation="v",
       |            measure=["relative"] * (len(y_values) - 1) + ["total"],
       |            x=x_values,
       |            y=y_values,
       |            textposition="outside",
       |            text=[f"{v:+}" for v in y_values],
       |            connector={"line": {"color": "rgb(63, 63, 63)"}}
       |        ))
       |
       |        fig.update_layout(showlegend=True, waterfallgap=0.3)
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode =
      s"""
         |from pytexera import *
         |
         |import plotly.graph_objects as go
         |import plotly.io
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>Waterfall chart is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("input table is empty.")}
         |            return
         |        ${createPlotlyFigure()}
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |""".stripMargin
    finalCode
  }

  // Specify the chart type as HTML visualization
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
