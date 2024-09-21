package edu.uci.ics.texera.workflow.operators.visualization.filledAreaPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

class FilledAreaPlotOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("X-axis Attribute")
  @JsonPropertyDescription("The attribute for your x-axis")
  @AutofillAttributeName
  var x: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y-axis Attribute")
  @JsonPropertyDescription("The attribute for your y-axis")
  @AutofillAttributeName
  var y: String = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Line Group")
  @JsonPropertyDescription("The attribute for group of each line")
  @AutofillAttributeName
  var lineGroup: String = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Color")
  @JsonPropertyDescription("Choose an attribute to color the plot")
  @AutofillAttributeName
  var color: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Split Plot by  Line Group")
  @JsonPropertyDescription("Do you want to split the graph")
  var facetColumn: Boolean = false

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
      "FilledAreaPlot",
      "Visualize data in filled area plot",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def createPlotlyFigure(): String = {
    assert(x.nonEmpty)
    assert(y.nonEmpty)

    if (facetColumn) {
      assert(lineGroup.nonEmpty)
    }

    val colorArg = if (color.nonEmpty) s""", color="$color"""" else ""
    val facetColumnArg = if (facetColumn) s""", facet_col="$lineGroup"""" else ""
    val lineGroupArg = if (lineGroup.nonEmpty) s""", line_group="$lineGroup"""" else ""
    val patternParam = if (pattern.nonEmpty) s""", pattern_shape="$pattern"""" else ""

    s"""
             |            fig = px.area(table, x="$x", y="$y"$colorArg$facetColumnArg$lineGroupArg$patternParam)
             |""".stripMargin
  }

  // The function below checks whether there are more than 5 percents of the groups have disjoint sets of x attributes.
  def performTableCheck(): String = {
    s"""
       |        error = ""
       |        if "$x" not in columns or "$y" not in columns:
       |            error = "missing attributes"
       |        elif "$lineGroup" != "":
       |            grouped = table.groupby("$lineGroup")
       |            x_values = None
       |
       |            tolerance = (len(grouped) // 100) * 5
       |            count = 0
       |
       |            for _, group in grouped:
       |                if x_values == None:
       |                    x_values = set(group["$x"].unique())
       |                elif set(group["$x"].unique()).intersection(x_values):
       |                    X_values = x_values.union(set(group["$x"].unique()))
       |                elif not set(group["$x"].unique()).intersection(x_values):
       |                    count += 1
       |                    if count > tolerance:
       |                        error = "X attributes not shared across groups"
       |""".stripMargin
  }

  override def generatePythonCode(): String = {
    val finalCode = s"""
         |from pytexera import *
         |
         |import plotly
         |import plotly.express as px
         |import plotly.graph_objects as go
         |import plotly.io
         |
         |class ProcessTableOperator(UDFTableOperator):
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        columns = list(table.columns)
         |        ${performTableCheck()}
         |
         |        if error == "":
         |            ${createPlotlyFigure()}
         |            fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
         |
         |            html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |            yield {'html-content': html}
         |        elif error == "X attributes not shared across groups":
         |
         |            html = '''<h1>Plot is not available, because:</h1>
         |                      <li>X attribute is not shared across all line groups</li>
         |                      </ul>'''
         |
         |            yield {'html-content': html}
         |        elif error == "missing attributes":
         |
         |            html = '''<h1>Plot is not available, because:</h1>
         |                      <li>X or Y attribute does not exist</li>
         |                      </ul>'''
         |
         |            yield {'html-content': html}
         |""".stripMargin
    finalCode
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}
