package edu.uci.ics.texera.workflow.operators.visualization.boxPlot

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

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "value": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class BoxPlotOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(defaultValue = "Box Plot Visual")
  @JsonSchemaTitle("Title")
  @JsonPropertyDescription("Add a title to your visualization")
  var title: String = ""

  @JsonProperty(value = "value", required = true)
  @JsonSchemaTitle("Value Column")
  @JsonPropertyDescription("Data Column for Boxplot")
  @AutofillAttributeName
  var value: String = ""

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Horizontal Orientation")
  @JsonPropertyDescription("Orientation Style")
  var orientation: Boolean = _

  @JsonProperty(
    value = "Quartile Method",
    required = true,
    defaultValue = "linear"
  )
  var quertiletype: BoxPlotQuartileFunction = _

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Box Plot",
      "Visualize data in a Box Plot. Boxplots are drawn as a box with a vertical line down the middle which is mean value, and has horizontal lines attached to each side (known as “whiskers”).",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def manipulateTable(): String = {
    assert(value.nonEmpty)

    s"""
       |        table = table.dropna(subset = ['$value']) #remove missing values
       |
       |""".stripMargin
  }

  def createPlotlyFigure(): String = {
    var horizontal = ""
    if (orientation) horizontal = "True"
    s"""
       |        if($horizontal):
       |            fig = px.box(table, x='$value',boxmode="overlay", points='all')
       |        else:
       |            fig = px.box(table, y='$value',boxmode="overlay", points='all')
       |        fig.update_traces(quartilemethod="${quertiletype.getQuartiletype}", jitter=0, col=1)
       |
       |
       |
       |""".stripMargin
  }

  override def generatePythonCode(): String = {

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
         |        return '''<h1>Box Plot is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty.")}
         |           return
         |        ${manipulateTable()}
         |        if table.empty:
         |           yield {'html-content': self.render_error("value column contains only non-positive numbers or nulls.")}
         |           return
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |        """.stripMargin
    finalCode
  }
  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
