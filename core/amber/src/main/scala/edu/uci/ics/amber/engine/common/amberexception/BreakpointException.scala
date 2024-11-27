package edu.uci.ics.amber.engine.common.amberexception

import edu.uci.ics.amber.core.WorkflowRuntimeException

class BreakpointException extends WorkflowRuntimeException("breakpoint triggered") {}
