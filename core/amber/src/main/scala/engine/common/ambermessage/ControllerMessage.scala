package engine.common.ambermessage

import engine.architecture.breakpoint.FaultedTuple
import engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import engine.architecture.controller.ControllerState
import engine.common.ambertag.OperatorIdentifier
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
