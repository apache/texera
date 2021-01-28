package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.principal.{OperatorState, OperatorStatistics}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.ActorRef
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ControllerEvent {

  case class WorkflowCompleted(
      // map from sink operator ID to the result list of tuples
      result: Map[String, List[ITuple]]
  )

  case class WorkflowPaused()

  case class WorkflowStatusUpdate(
      operatorStatistics: Map[String, OperatorStatistics]
  )

  case class ModifyLogicCompleted()

  case class BreakpointTriggered(
      report: mutable.HashMap[(ActorVirtualIdentity, FaultedTuple), ArrayBuffer[String]],
      operatorID: String = null
  )

  case class SkipTupleResponse()

  case class ErrorOccurred(error: WorkflowRuntimeError)

  case class ReportCurrentProcessingTuple(operatorID:String, tuple: Array[(ITuple, ActorVirtualIdentity)])

}
