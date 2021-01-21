package edu.uci.ics.amber.engine.common.amberexception

import edu.uci.ics.amber.error.WorkflowRuntimeError

class WorkflowRuntimeException(val runtimeError: WorkflowRuntimeError)
    extends RuntimeException(runtimeError.errorMessage)
    with Serializable {}
