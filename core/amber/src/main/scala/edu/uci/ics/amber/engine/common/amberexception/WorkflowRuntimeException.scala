package edu.uci.ics.amber.engine.common.amberexception

import edu.uci.ics.amber.error.WorkflowRuntimeError

case class WorkflowRuntimeException(runtimeError: WorkflowRuntimeError)
    extends RuntimeException
    with Serializable {}
