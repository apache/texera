package edu.uci.ics.amber.engine.common.amberexception

import edu.uci.ics.amber.error.WorkflowRuntimeError

class BreakpointException
    extends WorkflowRuntimeException(WorkflowRuntimeError("breakpoint triggered", "", Map())) {}
