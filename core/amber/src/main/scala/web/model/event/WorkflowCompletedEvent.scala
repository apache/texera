package web.model.event

import Engine.Architecture.Controller.ControllerEvent.WorkflowCompleted

import scala.collection.mutable

case class OperatorResult(operatorID: String, table: List[List[String]])

object WorkflowCompletedEvent {

  // transform results in amber tuple format to the format accepted by frontend
  def apply(workflowCompleted: WorkflowCompleted): WorkflowCompletedEvent = {
    val resultList = new mutable.MutableList[OperatorResult]
    workflowCompleted.result.foreach(pair => {
      val operatorID = pair._1
      val table = pair._2.map(tuple => tuple.toArray().map(v => v.toString).toList)
      resultList += OperatorResult(operatorID, table)
    })
    WorkflowCompletedEvent(resultList.toList)
  }
}

case class WorkflowCompletedEvent(result: List[OperatorResult]) extends TexeraWsEvent
