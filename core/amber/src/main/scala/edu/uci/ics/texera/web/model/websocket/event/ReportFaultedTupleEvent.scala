package edu.uci.ics.texera.web.model.websocket.event

import edu.uci.ics.texera.web.workflowruntimestate.BreakpointFault

case class ReportFaultedTupleEvent(
    report: Iterable[BreakpointFault],
    operatorID: String
) extends TexeraWebSocketEvent
