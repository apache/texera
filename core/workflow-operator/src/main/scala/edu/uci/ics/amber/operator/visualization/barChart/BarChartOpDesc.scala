package edu.uci.ics.amber.operator.visualization.barChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}

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
class BarChartOpDesc extends PythonOperatorDescriptor {

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

  @JsonProperty(required = false)
  @JsonSchemaTitle("Pattern")
  @JsonPropertyDescription("Add texture to the chart based on an attribute")
  @AutofillAttributeName
  var pattern: String = ""

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Bar Chart",
      "Visualize data in a Bar Chart",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
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

    var isPatternSelected = "False"
    if (pattern != "")
      isPatternSelected = "True"

    var isCategoryColumn = "False"
    if (categoryColumn != "No Selection")
      isCategoryColumn = "True"

    val finalCode =
      s"""
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
         |               fig = go.Figure(px.bar(table, y='$fields', x='$value', color="$categoryColumn" if $isCategoryColumn else None, pattern_shape="$pattern" if $isPatternSelected else None, orientation = 'h'))
         |           else:
         |               fig = go.Figure(px.bar(table, y='$value', x='$fields', color="$categoryColumn" if $isCategoryColumn else None, pattern_shape="$pattern" if $isPatternSelected else None))
         |           fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
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

}
