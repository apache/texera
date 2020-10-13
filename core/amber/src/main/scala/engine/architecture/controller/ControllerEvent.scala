package engine.architecture.controller

import engine.architecture.breakpoint.FaultedTuple
import engine.architecture.principal.{PrincipalState, PrincipalStatistics}
import engine.common.tuple.Tuple
import akka.actor.ActorRef

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ControllerEvent {

  case class WorkflowCompleted(
      // map from sink operator ID to the result list of tuples
      result: Map[String, List[Tuple]]
  )

  case class WorkflowPaused()

  case class WorkflowStatusUpdate(
      operatorStatistics: Map[String, PrincipalStatistics]
  )

  case class ModifyLogicCompleted()

  case class BreakpointTriggered(
      report: mutable.HashMap[(ActorRef, FaultedTuple), ArrayBuffer[String]],
      operatorID: String = null
  )

  case class SkipTupleResponse()

}
