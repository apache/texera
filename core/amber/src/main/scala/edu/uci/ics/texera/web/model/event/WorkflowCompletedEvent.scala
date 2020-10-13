package edu.uci.ics.texera.web.model.event

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowCompleted
import com.fasterxml.jackson.databind.node.ObjectNode
import edu.uci.ics.texera.workflow.common.tuple.TexeraTuple

import scala.collection.mutable

case class OperatorResult(operatorID: String, table: List[ObjectNode])

object WorkflowCompletedEvent {

  // transform results in amber tuple format to the format accepted by frontend
  def apply(workflowCompleted: WorkflowCompleted): WorkflowCompletedEvent = {
    val resultList = new mutable.MutableList[OperatorResult]
    workflowCompleted.result.foreach(pair => {
      val operatorID = pair._1
      val table = pair._2.map(tuple => tuple.asInstanceOf[TexeraTuple].asKeyValuePairJson())
      resultList += OperatorResult(operatorID, table)
    })
    WorkflowCompletedEvent(resultList.toList)
  }
}

case class WorkflowCompletedEvent(result: List[OperatorResult]) extends TexeraWsEvent
