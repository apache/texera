package web.model.event

import Engine.Architecture.Controller.ControllerEvent
import web.model.common.FaultedTupleFrontend

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class BreakpointFault(
    actorPath: String,
    faultedTuple: FaultedTupleFrontend,
    messages: ArrayBuffer[String]
)

object BreakpointTriggeredEvent {
  def apply(event: ControllerEvent.BreakpointTriggered): BreakpointTriggeredEvent = {
    val faults = new mutable.MutableList[BreakpointFault]()
    for (elem <- event.report) {
      val actorPath = elem._1._1.path.toSerializationFormat
      val faultedTuple = elem._1._2
      if (faultedTuple != null) {
        faults += BreakpointFault(actorPath, FaultedTupleFrontend.apply(faultedTuple), elem._2)
      }
    }
    BreakpointTriggeredEvent(faults, event.operatorID)
  }
}

case class BreakpointTriggeredEvent(
    report: mutable.MutableList[BreakpointFault],
    operatorID: String
) extends TexeraWsEvent
