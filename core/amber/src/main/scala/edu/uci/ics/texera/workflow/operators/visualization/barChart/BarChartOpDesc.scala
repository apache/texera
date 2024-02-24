package edu.uci.ics.texera.workflow.operators.visualization.barChart

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
class BarChartOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(defaultValue = "Bar Graph Visual")
  @JsonSchemaTitle("Title")
  @JsonPropertyDescription("Add a title to your visualization")
  var title: String = ""

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("the value associated with each category")
  @AutofillAttributeName
  var value: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Fields")
  @JsonPropertyDescription("Visualize categorical data in a Bar Chart")
  @AutofillAttributeName
  var fields: String = ""

  @JsonProperty(defaultValue = "No Selection", required = false)
  @JsonSchemaTitle("Category Column")
  @JsonPropertyDescription("Optional - Select a column to Color Code the Categories")
  @AutofillAttributeName
  var categoryColumn: String = ""

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Horizontal Orientation")
  @JsonPropertyDescription("Orientation Style")
  var horizontalOrientation: Boolean = _

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Bar Chart",
      "Visualize data in a Bar Chart",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def manipulateTable(): String = {
    assert(value.nonEmpty)
    assert(fields.nonEmpty)
    s"""
       |        table = table.dropna(subset = ['$value', '$fields']) #remove missing values
       |""".stripMargin
  }

  override def generatePythonCode(): String = {

    var isHorizontalOrientation = "False"
    if (horizontalOrientation)
      isHorizontalOrientation = "True"

    var isCategoryColumn = "False"
    if (categoryColumn != "No Selection")
      isCategoryColumn = "True"

    val finalCode = s"""
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
                        |    # Generate custom error message as html string
                        |    def render_error(self, error_msg) -> str:
                        |        return '''<h1>Bar chart is not available.</h1>
                        |                  <p>Reason is: {} </p>
                        |               '''.format(error_msg)
                        |
                        |    @overrides
                        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
                        |        ${manipulateTable()}
                        |        if not table.empty and '$fields' != '$value':
                        |           if $isHorizontalOrientation:
                        |               fig = go.Figure(px.bar(table, y='$fields', x='$value', color="$categoryColumn" if $isCategoryColumn else None, orientation = 'h', title='$title'))
                        |           else:
                        |               fig = go.Figure(px.bar(table, y='$value', x='$fields', color="$categoryColumn" if $isCategoryColumn else None, title='$title'))
                        |           html = plotly.io.to_html(fig, include_plotlyjs = 'cdn', auto_play = False)
                        |           # use latest plotly lib in html
                        |           #html = html.replace('https://cdn.plot.ly/plotly-2.3.1.min.js', 'https://cdn.plot.ly/plotly-2.18.2.min.js')
                        |        elif '$fields' == '$value':
                        |           html = self.render_error('Fields should not have the same value.')
                        |        elif table.empty:
                        |           html = self.render_error('Table should not have any empty/null values or fields.')
                        |        yield {'html-content':html}
                        |        """.stripMargin
    finalCode
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
