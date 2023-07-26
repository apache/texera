package edu.uci.ics.texera.workflow.operators.visualization.pieChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

//type constraint: value can only be numeric
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "value": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class PieChartOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("the value associated with slice of pie")
  @AutofillAttributeName
  var value: String = ""

  @JsonProperty(value = "names", required = true)
  @JsonSchemaTitle("Name Column(s)")
  @JsonPropertyDescription("the names of the slice of pie")
  @AutofillAttributeName
  var names: String = ""

  @JsonProperty(value = "title", required = false, defaultValue = "PieChart Visualization")
  @JsonSchemaTitle("Title")
  @JsonPropertyDescription("the title of this pie chart")
  var title: String = "PieChart Visualization"

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "PieChart",
      "Visualize data in a Pie Chart",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def manipulateTable(): String = {
    assert(value.nonEmpty)
    s"""
       |        table = table.dropna() #remove missing values
       |""".stripMargin
  }

  override def numWorkers() = 1

  def createPlotlyFigure(): String = {
    assert(value.nonEmpty)
    s"""
       |        fig = px.pie(table, names='$names', values='$value', title='$title')
       |        fig.update_layout(margin=dict(t=40, b=0, l =0, r=0))
       |""".stripMargin
  }

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String = {
    val final_code = s"""
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
                        |                  <p>Reasons is: {} </p>
                        |               '''.format(error_msg)
                        |
                        |    @overrides
                        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
                        |        original_table = table
                        |        if table.empty:
                        |           yield {'html-content': self.render_error("input table is empty.")}
                        |           return
                        |        ${manipulateTable()}
                        |        if table.empty:
                        |           yield {'html-content': self.render_error("value column contains only non-positive numbers.")}
                        |           return
                        |        duplicates = table.duplicated(subset=['$names'])
                        |        if duplicates.any():
                        |           yield {'html-content': self.render_error("duplicates in name column, need to aggregate")}
                        |           return
                        |        ${createPlotlyFigure()}
                        |        # convert fig to html content
                        |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
                        |        yield {'html-content': html}
                        |
                        |""".stripMargin
    final_code
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}