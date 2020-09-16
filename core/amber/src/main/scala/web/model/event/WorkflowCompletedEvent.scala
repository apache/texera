package web.model.event

import Engine.Architecture.Controller.ControllerEvent.WorkflowCompleted
import texera.common.workflow.TexeraWorkflowCompiler
import texera.operators.visualization.VisualizationOperator

import scala.collection.mutable

case class OperatorResult(operatorID: String, table: List[List[String]], chartType: String)

object WorkflowCompletedEvent {

  // transform results in amber tuple format to the format accepted by frontend
  def apply(workflowCompleted: WorkflowCompleted, texeraWorkflowCompiler: TexeraWorkflowCompiler): WorkflowCompletedEvent = {
    val resultList = new mutable.MutableList[OperatorResult]
    workflowCompleted.result.foreach(pair => {
      val sinkID = pair._1
      val table = pair._2.map(tuple => tuple.toArray().map(v => v.toString).toList)
      val sinkInputID = texeraWorkflowCompiler.texeraWorkflow.links
        .find(link => link.destination == sinkID)
        .get
        .origin
      val sinkInputOperator = texeraWorkflowCompiler.texeraWorkflow.operators.find(op => op.operatorID == sinkInputID)
        .get
      val chartType = sinkInputOperator match {
        case operator: VisualizationOperator => operator.chartType
        case _ => null
      }
      resultList += OperatorResult(sinkID, table, chartType)
    })
    WorkflowCompletedEvent(resultList.toList)
  }
}

case class WorkflowCompletedEvent(result: List[OperatorResult]) extends TexeraWsEvent
