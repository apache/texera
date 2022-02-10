package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.texera.web.workflowruntimestate.BreakpointFault

case class BreakpointTriggeredEvent(
    report: Iterable[BreakpointFault],
    operatorID: String
) extends TexeraWebSocketEvent
