package edu.uci.ics.texera.web.model.request

import edu.uci.ics.texera.workflow.common.workflow.TexeraBreakpoint

case class AddBreakpointRequest(operatorID: String, breakpoint: TexeraBreakpoint)
    extends TexeraWsRequest
