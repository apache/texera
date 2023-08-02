package edu.uci.ics.texera.workflow.operators.visualization.timeseries

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
    },
    "date": {
      "enum": ["timestamp"]
    }
  }
}
""")
class TimeSeriesOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "title", required = false)
  @JsonSchemaTitle("Plot title")
  @JsonPropertyDescription("The value for the plot tile")
  var title: String = ""

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("the value changing over time")
  @AutofillAttributeName
  var value: String = ""

  @JsonProperty(value = "yLabel", required = true)
  @JsonSchemaTitle("Y label")
  @JsonPropertyDescription("the value for the label of y axis")
  var yLabel: String = ""

  @JsonProperty(value = "date", required = true)
  @JsonSchemaTitle("Date Column")
  @JsonPropertyDescription("select the date column")
  @AutofillAttributeName
  var date: String = ""

  @JsonProperty(value = "xLabel", required = true)
  @JsonSchemaTitle("X label")
  @JsonPropertyDescription("the value for the label of x axis")
  var xLabel: String = ""

  @JsonProperty(value = "tick", required = true)
  @JsonSchemaTitle("tick")
  @JsonPropertyDescription(
    "The value for the tick interval. It should follow the format 'M<n>' n is the number of months and the plot would generate a tick every n month e.g. 'M1' render a tick every month, M2 for every two month etc."
  )
  var tick: String = ""

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "TimeSeries Visualizer",
      "Visualize data in a certain time period",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def manipulateTable(): String = {
    assert(value.nonEmpty)
    assert(date.nonEmpty)
    s"""
       |        table.dropna(subset=['$value','$date'], inplace=True)#remove missing values
       |        table = table.sort_values(by='$date', ascending=True)
       |""".stripMargin
  }

  override def numWorkers() = 1

  def createPlotlyFigure(): String = {
    assert(s"$tick".matches("^M[1-9][0-9]*$"))
    s"""
       |        fig = px.line(table, x='$date', y='$value', title='$title')
       |        fig.update_xaxes(dtick='$tick', title='$xLabel')
       |        fig.update_yaxes(title='$yLabel')
       |""".stripMargin
  }

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String = {
    val finalCode =
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
         |    # Generate custom error message as html string
         |    def render_error(self, error_msg) -> str:
         |        return '''<h1>TimeSeries is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |            yield {'html-content': self.render_error("input table is empty.")}
         |            return
         |        ${manipulateTable()}
         |        if table.empty:
         |            yield {'html-content': self.render_error("value column contains only nulls.")}
         |            return
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |""".stripMargin
    finalCode
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
