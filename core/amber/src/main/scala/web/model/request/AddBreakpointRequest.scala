package web.model.request

import texera.common.workflow.TexeraBreakpoint

case class AddBreakpointRequest(operatorID: String, breakpoint: TexeraBreakpoint) extends TexeraWsRequest

