package edu.uci.ics.texera.workflow.operators.visualization.dumbbellPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

//type constraint: measurementColumnName can only be a numeric column
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "measurementColumnName": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class DumbbellPlotOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "title", required = false, defaultValue = "DumbbellPlot Visualization")
  @JsonSchemaTitle("Title")
  @JsonPropertyDescription("the title of this dumbbell plots")
  var title: String = "DumbbellPlot Visualization"

  @JsonProperty(value = "categoryColumnName", required = true)
  @JsonSchemaTitle("Category Column Name")
  @JsonPropertyDescription("the name of the category column")
  @AutofillAttributeName
  var categoryColumnName: String = ""

  @JsonProperty(value = "startValue", required = true)
  @JsonSchemaTitle("Start Value")
  @JsonPropertyDescription("the start point value of each dumbbell")
  var startValue: String = ""

  @JsonProperty(value = "endValue", required = true)
  @JsonSchemaTitle("End Value")
  @JsonPropertyDescription("the end value of each dumbbell")
  var endValue: String = ""

  @JsonProperty(value = "measurementColumnName", required = true)
  @JsonSchemaTitle("Measurement Column Name")
  @JsonPropertyDescription("the name of the measurement column")
  @AutofillAttributeName
  var measurementColumnName: String = ""

  @JsonProperty(value = "comparedColumnName", required = true)
  @JsonSchemaTitle("Compared Column Name")
  @JsonPropertyDescription("the column name that is being compared")
  @AutofillAttributeName
  var comparedColumnName: String = ""

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "DumbbellPlot",
      "Visualize data in a Dumbbell Plots. A dumbbell plots (also known as a lollipop chart) is typically used to compare two distinct values or time points for the same entity.",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def createPlotlyFigure(): String = {
    val dumbbellValues = startValue + ", " + endValue

    s"""
     |
     |        entityNames = list(table['${comparedColumnName}'].unique())
     |        categoryValues = [${dumbbellValues}]
     |        filtered_table = table[(table['${comparedColumnName}'].isin(entityNames)) &
     |                    (table['${categoryColumnName}'].isin(categoryValues))]
     |
     |        # Create the dumbbell plot using Plotly
     |        fig = go.Figure()
     |
     |        for entity in entityNames:
     |          entity_data = filtered_table[filtered_table['${comparedColumnName}'] == entity]
     |          fig.add_trace(go.Scatter(x=entity_data['${measurementColumnName}'],
     |                             y=[entity]*2,
     |                             mode='lines+markers+text',
     |                             name=entity,
     |                             text=entity_data['${categoryColumnName}'],
     |                             textposition='top center',
     |                             marker=dict(size=10)))
     |
     |          fig.update_layout(title="${title}",
     |                  xaxis_title="${measurementColumnName}",
     |                  yaxis_title="${comparedColumnName}",
     |                  yaxis=dict(categoryorder='array', categoryarray=entityNames))
     |""".stripMargin
  }

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String = {
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
     |        return '''<h1>DumbbellPlot is not available.</h1>
     |                  <p>Reason is: {} </p>
     |               '''.format(error_msg)
     |
     |    @overrides
     |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
     |        if table.empty:
     |           yield {'html-content': self.render_error("input table is empty.")}
     |           return
     |        ${createPlotlyFigure()}
     |        # convert fig to html content
     |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
     |        yield {'html-content': html}
     |
     |""".stripMargin
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
