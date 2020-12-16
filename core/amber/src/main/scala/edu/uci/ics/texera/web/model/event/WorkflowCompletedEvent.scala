package edu.uci.ics.texera.web.model.event

import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowCompleted
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationOperator

import scala.collection.mutable

case class OperatorResult(operatorID: String, table: List[ObjectNode], chartType: String)

object WorkflowCompletedEvent {

  // transform results in amber tuple format to the format accepted by frontend
  def apply(
      workflowCompleted: WorkflowCompleted,
      workflowCompiler: WorkflowCompiler
  ): WorkflowCompletedEvent = {
    val resultList = new mutable.MutableList[OperatorResult]
    workflowCompleted.result.foreach(pair => {
      val operatorID = pair._1
      val table = pair._2.map(tuple => tuple.asInstanceOf[Tuple].asKeyValuePairJson())

      // add chartType to result
      val precedentOpID =
        workflowCompiler.workflowInfo.links.find(link => link.destination == operatorID).get.origin
      val precedentOp =
        workflowCompiler.workflowInfo.operators.find(op => op.operatorID == precedentOpID).get
      val chartType = precedentOp match {
        case operator: VisualizationOperator => operator.chartType()
        case _                               => null
      }

      resultList += OperatorResult(operatorID, table, chartType)
    })
    WorkflowCompletedEvent(resultList.toList)
  }
}

case class WorkflowCompletedEvent(result: List[OperatorResult]) extends TexeraWebSocketEvent
