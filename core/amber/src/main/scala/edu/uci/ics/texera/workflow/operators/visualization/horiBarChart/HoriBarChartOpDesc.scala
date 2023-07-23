package edu.uci.ics.texera.workflow.operators.visualization.horiBarChart

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
class HoriBarChartOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("the value associated with each category")
  @AutofillAttributeName
  var value: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Fields")
  @JsonPropertyDescription("Visualize categorical data in a Bar Chart")
  @AutofillAttributeName
  var fields: String = _

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Horizontal Orientation")
  @JsonPropertyDescription("Orientation Style")
  var Orientation: Boolean = _

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hori-BarChart",
      "Visualize data in a Horizontal Bar Chart",
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
    s"""
       |fig = px.bar(table, y= '$fields', x='$value', orientation = 'h')
       |""".stripMargin
  }

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String = {
    var truthy = ""
    if (Orientation) truthy = "True"
    val final_code = s"""
                        |from pytexera import *
                        |
                        |import plotly.express as px
                        |import pandas as pd
                        |import plotly.graph_objects as go
                        |import plotly.io
                        |import json
                        |import pickle
                        |import plotly
                        |
                        |class ProcessTableOperator(UDFTableOperator):
                        |
                        |    @overrides
                        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
                        |        ${manipulateTable()}
                        |        print(table)
                        |        if ($truthy):
                        |           fig = go.Figure(px.bar(table, y='$fields', x='$value', orientation = 'h'))
                        |        else:
                        |           fig = go.Figure(px.bar(table, y='$value', x='$fields'))
                        |        html = plotly.io.to_html(fig, include_plotlyjs = 'cdn', auto_play = False)
                        |        # use latest plotly lib in html
                        |        #html = html.replace('https://cdn.plot.ly/plotly-2.3.1.min.js', 'https://cdn.plot.ly/plotly-2.18.2.min.js')
                        |        yield {'html-content':html}
                        |        """.stripMargin
    final_code
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}