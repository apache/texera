package edu.uci.ics.texera.workflow.operators.visualization.table

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.visualization.{VisualizationConstants, VisualizationOperator}

class TableOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonProperty(value = "title", required = false, defaultValue = "Table Visualization")
  @JsonSchemaTitle("Title")
  @JsonPropertyDescription("the title of this table")
  var title: String = "Table Visualization"

  @JsonSchemaTitle("Output columns")
  @JsonPropertyDescription("the column name(s) of the table chart")
  var tableColumnNameUnits: List[TableColumnNameUnit] = List()

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema.newBuilder.add(new Attribute("html-content", AttributeType.STRING)).build
  }

  override def operatorInfo: OperatorInfo = OperatorInfo(
    "Table",
    "A Table chart is a type of grid representation that displays data in rows and columns. " +
      "The chart organizes categories of information along the vertical axis (rows) and specific data points or metrics along the horizontal axis (columns). " +
      "The intersection of a row and a column in the grid provides the specific value or data point related to that category and metric.",
    OperatorGroupConstants.VISUALIZATION_GROUP,
    inputPorts = List(InputPort()),
    outputPorts = List(OutputPort())
  )

  // create a boolean val to indicate if table is error free
  def createTableErrFreeFlag(): String = {
    val tableColumnNames = tableColumnNameUnits.map(unit => s"'${unit.columnName}'").mkString(", ")
    s"""
       |        is_table_err_free = set([${tableColumnNames}]).issubset(table.columns)
       |""".stripMargin
  }

  def createPlotlyFigure(): String = {

    val tableColumnNames = tableColumnNameUnits.map(unit => s"'${unit.columnName}'").mkString(", ")
    val tableColumnValues = tableColumnNameUnits.map(unit => s"table['${unit.columnName}'].tolist()").mkString(", ")
    s"""
       |        fig = go.Figure(data=[go.Table(header=dict(values=[${tableColumnNames}]),
       |                        cells=dict(values=[${tableColumnValues}]))])
       |        fig.update_layout(title_text='${title}', title_x=0.5, title_xanchor='center')
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
         |        return '''<h1>Table is not available.</h1>
         |                  <p>Reason is: {} </p>
         |               '''.format(error_msg)
         |
         |    @overrides
         |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
         |        original_table = table
         |        if table.empty:
         |           yield {'html-content': self.render_error("input table is empty.")}
         |           return
         |        if table.empty:
         |           yield {'html-content': self.render_error("value column contains only non-positive numbers.")}
         |           return
         |        ${createTableErrFreeFlag()}
         |        if not is_table_err_free:
         |           yield {'html-content': self.render_error("Some given column names do not exist in the input data")}
         |           return
         |
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |
         |""".stripMargin
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ

  override def numWorkers(): Int = 1
}
