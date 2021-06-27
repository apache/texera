package edu.uci.ics.texera.web.model.event

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.principal.{OperatorState, OperatorStatistics}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler

object WebOperatorStatistics {

  def apply(
      operatorID: String,
      operatorStatistics: OperatorStatistics,
      dirtyPageIndices: Map[String, List[Int]],
      workflowCompiler: WorkflowCompiler
  ): WebOperatorStatistics = {
    val chartType = OperatorResult.getChartType(operatorID, workflowCompiler)
    // TODO: temporary fix to make it compile, this will be reverted soon in a subsequent PR
    val results = Option
      .empty[List[ITuple]]
      // if chartType is present, then send all results
      // else (normal view result table), then send empty list
      //   (pagination will take care of sending actual result)
      .map(r =>
        chartType match {
          case Some(_) => OperatorResult.fromTuple(operatorID, r, chartType, r.size)
          case None    => OperatorResult.fromTuple(operatorID, List.empty, chartType, r.size)
        }
      )

    WebOperatorStatistics(
      operatorStatistics.operatorState,
      operatorStatistics.aggregatedInputRowCount,
      operatorStatistics.aggregatedOutputRowCount,
      results,
      dirtyPageIndices.get(operatorID)
    )
  }

}

case class WebOperatorStatistics(
    operatorState: OperatorState,
    aggregatedInputRowCount: Long,
    aggregatedOutputRowCount: Long,
    aggregatedOutputResults: Option[OperatorResult], // in case of a sink operator
    aggregatedOutputResultDirtyPageIndices: Option[List[Int]]
)

object WebWorkflowStatusUpdateEvent {
  def apply(
      update: WorkflowStatusUpdate,
      sinkOpDirtyPageIndices: Map[String, List[Int]],
      workflowCompiler: WorkflowCompiler
  ): WebWorkflowStatusUpdateEvent = {
    WebWorkflowStatusUpdateEvent(
      update.operatorStatistics.map(e =>
        (e._1, WebOperatorStatistics.apply(e._1, e._2, sinkOpDirtyPageIndices, workflowCompiler))
      )
    )
  }
}

case class WebWorkflowStatusUpdateEvent(operatorStatistics: Map[String, WebOperatorStatistics])
    extends TexeraWebSocketEvent
