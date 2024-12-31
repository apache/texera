package edu.uci.ics.amber.operator.visualization.ScatterMatrixChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList
}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "value": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class ScatterMatrixChartOpDesc extends PythonOperatorDescriptor {

  @JsonProperty(value = "Selected Attributes", required = true)
  @JsonSchemaTitle("Selected Attributes")
  @JsonPropertyDescription("the axes of each scatter plot in the matrix.")
  @AutofillAttributeNameList
  var selectedAttributes: List[String] = _

  @JsonProperty(value = "Color", required = true)
  @JsonSchemaTitle("Color Column")
  @JsonPropertyDescription("column to color points")
  @AutofillAttributeName
  var color: String = ""

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    val outputSchema = Schema
      .builder()
      .add(new Attribute("html-content", AttributeType.STRING))
      .build()
    Map(operatorInfo.outputPorts.head.id -> outputSchema)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Scatter Matrix Chart",
      "Visualize datasets in a Scatter Matrix",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
    )

  def createPlotlyFigure(): String = {
    assert(selectedAttributes.nonEmpty)

    val list_Attributes = selectedAttributes.map(attribute => s""""$attribute"""").mkString(",")
    s"""
       |        fig = px.scatter_matrix(table, dimensions=[$list_Attributes], color='$color')
       |        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
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
         |import numpy as np
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |
         |""".stripMargin
    finalcode
  }

}
