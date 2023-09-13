package edu.uci.ics.texera.workflow.operators.visualization.table

import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.visualization.{VisualizationConstants, VisualizationOperator}

class TableOpDesc extends VisualizationOperator with PythonOperatorDescriptor {

  @JsonSchemaTitle("Output columns")
  var tableHeaderUnits: List[TableHeaderUnit] = List()

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

  def createPlotlyFigure(): String = {

    val tableHeaders = tableHeaderUnits.map(unit => s"'${unit.attributeName}'").mkString(", ")
    val tableColumns = tableHeaderUnits.map(unit => s"table['${unit.attributeName}'].tolist()").mkString(", ")
    s"""
       |        fig = go.Figure(data=[go.Table(header=dict(values=[${tableHeaders}]),
       |                        cells=dict(values=[${tableColumns}]))])
       |""".stripMargin
  }

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String = {
    val finalcode =
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
         |        ${createPlotlyFigure()}
         |        # convert fig to html content
         |        html = plotly.io.to_html(fig, include_plotlyjs='cdn', auto_play=False)
         |        yield {'html-content': html}
         |
         |""".stripMargin
    System.out.print(finalcode)
    finalcode
  }

  override def chartType(): String = VisualizationConstants.HTML_VIZ

  override def numWorkers(): Int = 1
}
