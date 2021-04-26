package edu.uci.ics.texera.web.model.event

import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowCompleted
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator

import scala.collection.mutable

object OperatorResult {
  def getChartType(operatorID: String, workflowCompiler: WorkflowCompiler): Option[String] = {
    val outLinks =
      workflowCompiler.workflowInfo.links.filter(link => link.origin.operatorID == operatorID)
    val isSink = outLinks.isEmpty

    if (!isSink) {
      return None
    }

    // add chartType to result
    val precedentOpID =
      workflowCompiler.workflowInfo.links
        .find(link => link.destination.operatorID == operatorID)
        .get
        .origin
    val precedentOp =
      workflowCompiler.workflowInfo.operators
        .find(op => op.operatorID == precedentOpID.operatorID)
        .get
    precedentOp match {
      case operator: VisualizationOperator => Option.apply(operator.chartType())
      case _                               => Option.empty
    }
  }

  def fromTuple(
      operatorID: String,
      table: List[ITuple],
      chartType: Option[String],
      totalRowCount: Int
  ): OperatorResult = {
    OperatorResult(
      operatorID,
      table.map(t => t.asInstanceOf[Tuple].asKeyValuePairJson()),
      chartType,
      totalRowCount
    )
  }
}

case class OperatorResult(
    operatorID: String,
    table: List[ObjectNode],
    chartType: Option[String],
    totalRowCount: Int
)

object WorkflowCompletedEvent {
  val defaultPageSize = 10

  // transform results in amber tuple format to the format accepted by frontend
  def apply(
      workflowCompleted: WorkflowCompleted,
      workflowCompiler: WorkflowCompiler
  ): WorkflowCompletedEvent = {
    val resultList = new mutable.MutableList[OperatorResult]
    for ((operatorID, resultTuples) <- workflowCompleted.result) {
      val chartType = OperatorResult.getChartType(operatorID, workflowCompiler)

      var table = resultTuples
      // if not visualization result, then only return first page results
      if (chartType.isEmpty) {
        table = resultTuples.slice(0, defaultPageSize)
      }

      resultList += OperatorResult.fromTuple(operatorID, table, chartType, resultTuples.length)
    }
    WorkflowCompletedEvent(resultList.toList)
  }
}

case class WorkflowCompletedEvent(result: List[OperatorResult]) extends TexeraWebSocketEvent
