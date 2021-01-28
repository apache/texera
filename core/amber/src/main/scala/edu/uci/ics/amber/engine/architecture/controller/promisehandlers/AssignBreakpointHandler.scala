package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object AssignBreakpointHandler{
  final case class AssignBreakpoint[T<: LocalBreakpoint](breakpoint:GlobalBreakpoint[T]) extends ControlCommand[CommandCompleted]
}


trait AssignBreakpointHandler {

}
