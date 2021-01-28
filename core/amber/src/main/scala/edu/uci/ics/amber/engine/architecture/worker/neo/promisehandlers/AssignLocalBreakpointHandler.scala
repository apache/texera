package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.AssignLocalBreakpointHandler.AssignLocalBreakpoint
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object AssignLocalBreakpointHandler{
  final case class AssignLocalBreakpoint(bp:LocalBreakpoint) extends ControlCommand[CommandCompleted]
}


trait AssignLocalBreakpointHandler {
  this:WorkerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:AssignLocalBreakpoint, sender) =>
      breakpointManager.registerOrReplaceBreakpoint(msg.bp)
      CommandCompleted()
  }
}
