package edu.uci.ics.texera.workflow.operators.visualization.table

import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.PythonOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.visualization.{VisualizationConstants, VisualizationOperator}

class TableOpDesc extends VisualizationOperator with PythonOperatorDescriptor {
  override def chartType(): String = VisualizationConstants.HTML_VIZ

  override def operatorInfo: OperatorInfo = OperatorInfo(
    "Table",
    "A Table chart is a type of grid representation that displays data in rows and columns. " +
      "The chart organizes categories of information along the vertical axis (rows) and specific data points or metrics along the horizontal axis (columns). " +
      "The intersection of a row and a column in the grid provides the specific value or data point related to that category and metric.",
    OperatorGroupConstants.VISUALIZATION_GROUP,
    inputPorts = List(InputPort()),
    outputPorts = List(OutputPort())
  )

  override def getOutputSchema(schemas: Array[Schema]): Schema = ???

  override def numWorkers(): Int = 1

  override def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String = ???

}
