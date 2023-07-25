package edu.uci.ics.texera.workflow.operators.visualization.ganttChart

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


@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "task": {
      "enum": ["string"]
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

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Gantt Chart Visualizer",
      "Visualize data in a Gantt Chart",
      OperatorGroupConstants.VISUALIZATION_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  def manipulateTable(): String = {
    assert(task.nonEmpty)
    s"""
       |        table = table.dropna() #remove missing values
       |""".stripMargin
  }

  override def numWorkers() = 1




  def createPlotlyFigure(): String = {
    assert(task.nonEmpty)
    s"""
       |           fig = px.timeline(table, x_start="$start", x_end="$finish", y="$task", color="$color")
       |           fig.update_yaxes(autorange="reversed")
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
                        |
                        |    @overrides
                        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
                        |        ${manipulateTable()}
                        |        if not table.empty:
                        |           ${createPlotlyFigure()}
                        |           # convert fig to html content
                        |           html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
                        |           yield {'html-content': html}
                        |        else:
                        |           html = '''<h1>PieChart is not available.</h1>
                        |                     <p>Possible reasons are:</p>
                        |                     <ul>
                        |                     <li>input table is empty</li>
                        |                     <li>value column contains only non-positive numbers</li>
                        |                     <li>value column is not available</li>
                        |                     </ul>'''
                        |           yield {'html-content': html}
                        |""".stripMargin
    final_code
  }

  // make the chart type to html visualization so it can be recognized by both backend and frontend.
  override def chartType(): String = VisualizationConstants.HTML_VIZ
}