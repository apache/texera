package Engine.Architecture.Controller

import Engine.Architecture.Breakpoint.FaultedTuple
import Engine.Architecture.Principal.{PrincipalState, PrincipalStatistics}
import Engine.Common.AmberTuple.Tuple
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
