package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.principal.{OperatorResult, OperatorStatistics}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.collection.mutable

object ControllerEvent {

  case class WorkflowCompleted(
      // map from sink operator ID to the result list of tuples
      result: Map[String, OperatorResult]
  )

  case class WorkflowPaused()

  case class WorkflowStatusUpdate(
      operatorStatistics: Map[String, OperatorStatistics]
  )

  case class WorkflowResultUpdate(operatorResults: Map[String, OperatorResult])

  case class ModifyLogicCompleted()

  case class BreakpointTriggered(
      report: mutable.HashMap[(ActorVirtualIdentity, FaultedTuple), Array[String]],
      operatorID: String = null
  )

  case class PythonPrintTriggered(
      message: String,
      operatorID: String = null
  )

  case class SkipTupleResponse()

  case class ErrorOccurred(error: Throwable)

  case class ReportCurrentProcessingTuple(
      operatorID: String,
      tuple: Array[(ITuple, ActorVirtualIdentity)]
  )

}
