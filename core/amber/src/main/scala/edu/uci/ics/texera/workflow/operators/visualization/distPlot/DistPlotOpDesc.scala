package edu.uci.ics.texera.workflow.operators.visualization.distPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

class DistPlotOpDesc extends VisualizationOperator with PythonOperatorDescriptor {
  // x, y, color, marginal (box, violin, rug), hover_data (set by us)
  @JsonProperty(required = true)
  @JsonSchemaTitle("X-axis Value")
  @JsonPropertyDescription("X-axis value for distribution plot.")
  @AutofillAttributeName
  var xAxis: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y-axis Value")
  @JsonPropertyDescription("Y-axis value for distribution plot.")
  @AutofillAttributeName
  var yAxis: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Column")
  @JsonPropertyDescription("Attribute used to differentiate data.")
  @AutofillAttributeName
  var color: String = ""

  @JsonProperty(required = true, defaultValue = "rug")
  @JsonSchemaTitle("Distribution Type")
  @JsonPropertyDescription("Distribution type (rug, box, violin).") // box, violin, rug
  var marginal: String = ""

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Distribution Plot",
      "Visualize and compare data in a distribution plot (box, violin, rug)",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )


  def createPlotlyFigure(): String = {
    s"""
       |        fig = px.histogram(table, x="$xAxis", y="$yAxis", color="$color", marginal="$marginal", hover_data=table.columns)
       """.stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode = s"""
                       |from pytexera import *
                       |
                       |import plotly.express as px
                       |import plotly.io
                       |
                       |class ProcessTableOperator(UDFTableOperator):
                       |
                       |    def render_error(self, error_msg):
                       |        return '''<h1>DistPlot is not available.</h1>
                       |                  <p>Reasons are: {} </p>
                       |               '''.format(error_msg)
                       |
                       |    @overrides
                       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
                       |        if table.empty:
                       |            yield {'html-content': self.render_error("Input table is empty.")}
                       |            return
                       |        ${createPlotlyFigure()}
                       |        if table.empty:
                       |            yield {'html-content': self.render_error("No valid rows left (every row has at least 1 missing value).")}
                       |            return
                       |        # convert fig to html content
                       |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
                       |        yield {'html-content': html}
                       |""".stripMargin
    finalCode
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
