package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent
import edu.uci.ics.texera.web.model.common.FaultedTupleFrontend
import edu.uci.ics.texera.web.model.websocket.event.BreakpointTriggeredEvent.BreakpointFault

import scala.collection.mutable

object BreakpointTriggeredEvent {
  def apply(event: ControllerEvent.BreakpointTriggered): BreakpointTriggeredEvent = {
    val faults = new mutable.MutableList[BreakpointFault]()
    for (elem <- event.report) {
      val actorPath = elem._1._1.toString
      val faultedTuple = elem._1._2
      if (faultedTuple != null) {
        faults += BreakpointFault(actorPath, FaultedTupleFrontend.apply(faultedTuple), elem._2)
      }
    }
    BreakpointTriggeredEvent(faults, event.operatorID)
  }
  case class BreakpointFault(
      actorPath: String,
      faultedTuple: FaultedTupleFrontend,
      messages: Array[String]
  )
}

case class BreakpointTriggeredEvent(
    report: mutable.MutableList[BreakpointFault],
    operatorID: String
) extends TexeraWebSocketEvent
