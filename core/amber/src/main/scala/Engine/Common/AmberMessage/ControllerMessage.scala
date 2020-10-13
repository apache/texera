package Engine.Common.AmberMessage

import Engine.Architecture.Breakpoint.FaultedTuple
import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.Breakpoint.LocalBreakpoint.LocalBreakpoint
import Engine.Architecture.Controller.ControllerState
import Engine.Common.AmberTag.OperatorIdentifier
import akka.actor.ActorRef

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ControllerMessage {

  final case class AckedControllerInitialization()

  final case class ContinuedInitialization()

  final case class ReportState(controllerState: ControllerState.Value)

  final case class ReportGlobalBreakpointTriggered(
      report: mutable.HashMap[(ActorRef, FaultedTuple), ArrayBuffer[String]],
      operatorID: String = null
  )

  final case class PassBreakpointTo(operatorID: String, breakpoint: GlobalBreakpoint)

}
