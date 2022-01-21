package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.texera.web.model.common.FaultedTupleFrontend

case class BreakpointFault(
    actorPath: String,
    faultedTuple: FaultedTupleFrontend,
    messages: Array[String]
)

case class BreakpointTriggeredEvent(
    report: Array[BreakpointFault],
    operatorID: String
) extends TexeraWebSocketEvent
