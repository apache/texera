package edu.uci.ics.texera.workflow.operators.visualization.ScatterMatrix

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.texera.workflow.common.metadata.annotations.{AutofillAttributeName, AutofillAttributeNameList}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.operators.visualization.{VisualizationConstants, VisualizationOperator}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "value": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class ScatterMatrixOpDesc extends VisualizationOperator with PythonOperatorDescriptor {



  @JsonProperty("Selected Attributes")
  @JsonSchemaTitle("The variables represented on the axes of each scatterplot in the matrix. ")
  @JsonPropertyDescription("Selected Attributes")
  @AutofillAttributeNameList
  var selectedAttributes: List[String] = _

  @JsonProperty(value = "name", required = true)
  @JsonSchemaTitle("Name Column")
  @JsonPropertyDescription("the name of the classification label")
  @AutofillAttributeName
  var name: String = ""

  @JsonProperty(value = "title", required = false, defaultValue = "Scatter Matrix")
  @JsonSchemaTitle("Title")
  @JsonPropertyDescription("the title of this matrix")
  var title: String = "Scatter Matrix"

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "ScatterMatrix",
      "Visualize datasets in a Scatter Matrix",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )


  def createPlotlyFigure(): String = {
    assert(selectedAttributes.nonEmpty)

    val list_words = selectedAttributes.map(word => s""""$word"""").mkString(",")
    s"""
       |        fig = px.scatter_matrix(table, dimensions=[$list_words], color='$name')
       |        fig.update_layout(
       |        title='$title',
       |        width=800,
       |        height=800
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
         |import numpy as np
         |
         |class ProcessTableOperator(UDFTableOperator):
         |    def render_error(self, error_msg):
         |        return '''<h1>PieChart is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        original_table = table
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
