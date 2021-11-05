package edu.uci.ics.texera.web.model.websocket.request

import edu.uci.ics.texera.workflow.common.workflow.Breakpoint

case class AddBreakpointRequest(operatorID: String, breakpoint: Breakpoint)
    extends TexeraWebSocketRequest
