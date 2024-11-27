package edu.uci.ics.amber.operator.visualization.dumbbellPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.visualization.{VisualizationConstants, VisualizationOperator}

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
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

  @JsonProperty(value = "categoryColumnName", required = true)
  @JsonSchemaTitle("Category Column Name")
  @JsonPropertyDescription("the name of the category column")
  @AutofillAttributeName
  var categoryColumnName: String = ""

  @JsonProperty(value = "dumbbellStartValue", required = true)
  @JsonSchemaTitle("Dumbbell Start Value")
  @JsonPropertyDescription("the start point value of each dumbbell")
  var dumbbellStartValue: String = ""

  @JsonProperty(value = "dumbbellEndValue", required = true)
  @JsonSchemaTitle("Dumbbell End Value")
  @JsonPropertyDescription("the end value of each dumbbell")
  var dumbbellEndValue: String = ""

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

  @JsonProperty(value = "dots", required = false)
  var dots: util.List[DumbbellDotConfig] = _

  @JsonProperty(value = "showLegends", required = false)
  @JsonSchemaTitle("Show Legends?")
  @JsonPropertyDescription("whether show legends in the graph")
  var showLegends: Boolean = false;

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.builder().add(new Attribute("html-content", AttributeType.STRING)).build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "DumbbellPlot",
      "Visualize data in a Dumbbell Plots. A dumbbell plots (also known as a lollipop chart) is typically used to compare two distinct values or time points for the same entity.",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def createPlotlyDumbbellLineFigure(): String = {
    val dumbbellValues = dumbbellStartValue + ", " + dumbbellEndValue
    var showLegendsOption = "showlegend=False"
    if (showLegends) {
      showLegendsOption = "showlegend=True"
    }
    s"""
       |
       |        entityNames = list(table['${comparedColumnName}'].unique())
       |        entityNames = sorted(entityNames, reverse=True)
       |        categoryValues = [${dumbbellValues}]
       |        filtered_table = table[(table['${comparedColumnName}'].isin(entityNames)) &
       |                    (table['${categoryColumnName}'].isin(categoryValues))]
       |
       |        # Create the dumbbell line using Plotly
       |        fig = go.Figure()
       |        color = 'black'
       |        for entity in entityNames:
       |          entity_data = filtered_table[filtered_table['${comparedColumnName}'] == entity]
       |          fig.add_trace(go.Scatter(x=entity_data['${measurementColumnName}'],
       |                             y=[entity]*len(entity_data),
       |                             mode='lines',
       |                             name=entity,
       |                             line=dict(color=color)))
       |
       |          fig.update_layout(xaxis_title="${measurementColumnName}",
       |                  yaxis_title="${comparedColumnName}",
       |                  yaxis=dict(categoryorder='array', categoryarray=entityNames),
       |                  ${showLegendsOption}
       |                  )
       |""".stripMargin
  }

  def addPlotlyDots(): String = {

    var dotColumnNames = ""
    if (dots != null && dots.size() != 0) {
      dotColumnNames = dots.asScala
        .map { dot =>
          s"'${dot.dotValue}'"
        }
        .mkString(",")
    }

    s"""
       |        dotColumnNames = [${dotColumnNames}]
       |        if len(dotColumnNames) > 0:
       |          for dotColumn in dotColumnNames:
       |              # Extract dot data for each entity
       |              for entity in entityNames:
       |                  entity_dot_data = filtered_table[filtered_table['${comparedColumnName}'] == entity]
       |                  # Extract X and Y values for the dot
       |                  x_values = entity_dot_data[dotColumn].values
       |                  y_values = [entity] * len(x_values)
       |                  # Add scatter plot for dots
       |                  fig.add_trace(go.Scatter(x=x_values, y=y_values,
       |                                         mode='markers',
       |                                         name=entity + ' ' + dotColumn,
       |                                         marker=dict(color='black', size=5)))  # Customize color and size as needed
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
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
       |        ${createPlotlyDumbbellLineFigure()}
       |        ${addPlotlyDots()}
       |        # convert fig to html content
       |        fig.update_layout(margin=dict(l=0, r=0, b=60, t=0))
       |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
       |        yield {'html-content': html}
       |
       |""".stripMargin
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
