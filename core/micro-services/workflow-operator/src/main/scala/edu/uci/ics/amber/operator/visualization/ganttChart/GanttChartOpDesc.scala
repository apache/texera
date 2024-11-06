package edu.uci.ics.amber.operator.visualization.ganttChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.OperatorInfo
import edu.uci.ics.amber.operator.metadata.OperatorGroupConstants
import edu.uci.ics.amber.operator.metadata.annotation.AutofillAttributeName
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}
import edu.uci.ics.amber.operator.visualization.{VisualizationConstants, VisualizationOperator}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "start": {
      "enum": ["timestamp"]
    },
    "finish": {
      "enum": ["timestamp"]
    }
  }
}
""")
class GanttChartOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "start", required = true)
  @JsonSchemaTitle("Start Datetime Column")
  @JsonPropertyDescription("the start timestamp of the task")
  @AutofillAttributeName
  var start: String = ""

  @JsonProperty(value = "finish", required = true)
  @JsonSchemaTitle("Finish Datetime Column")
  @JsonPropertyDescription("the end timestamp of the task")
  @AutofillAttributeName
  var finish: String = ""

  @JsonProperty(value = "task", required = true)
  @JsonSchemaTitle("Task Column")
  @JsonPropertyDescription("the name of the task")
  @AutofillAttributeName
  var task: String = ""

  @JsonProperty(value = "color", required = false)
  @JsonSchemaTitle("Color Column")
  @JsonPropertyDescription("column to color tasks")
  @AutofillAttributeName
  var color: String = ""

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
      "Gantt Chart",
      "A Gantt chart is a type of bar chart that illustrates a project schedule. The chart lists the tasks to be performed on the vertical axis, and time intervals on the horizontal axis. The width of the horizontal bars in the graph shows the duration of each activity.",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def manipulateTable(): String = {
    val optionalFilterTable = if (color.nonEmpty) s"&(table['$color'].notnull())" else ""
    s"""
       |        table = table[(table["$start"].notnull())&(table["$finish"].notnull())&(table["$finish"].notnull())$optionalFilterTable].copy()
       |""".stripMargin
  }

  def createPlotlyFigure(): String = {
    val colorSetting = if (color.nonEmpty) s", color='$color'" else ""
    val patternParam = if (pattern.nonEmpty) s", pattern_shape='$pattern'" else ""

    s"""
        |        fig = px.timeline(table, x_start='$start', x_end='$finish', y='$task' $colorSetting $patternParam)
        |        fig.update_yaxes(autorange='reversed')
        |        fig.update_layout(margin=dict(t=0, b=0, l=0, r=0))
        |""".stripMargin

  }

  override def generatePythonCode(): String = {
    val finalCode = s"""
                        |from pytexera import *
                        |
                        |import plotly.express as px
                        |import plotly.graph_objects as go
                        |import plotly.io
                        |import numpy as np
                        |
                        |class ProcessTableOperator(UDFTableOperator):
                        |    def render_error(self, error_msg):
                        |        return '''<h1>Gantt Chart is not available.</h1>
                        |                  <p>Reason: {} </p>
                        |               '''.format(error_msg)
                        |
                        |    @overrides
                        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
                        |        if table.empty:
                        |           yield {'html-content': self.render_error("Input table is empty.")}
                        |           return
                        |        ${manipulateTable()}
                        |        if table.empty:
                        |           yield {'html-content': self.render_error("One or more of your input columns have all missing values")}
                        |           return
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
