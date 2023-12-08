package edu.uci.ics.texera.workflow.common
import org.jooq.types.UInteger

class WorkflowContext(
    var jobId: String = "default",
    var userId: Option[UInteger] = None,
    var wid: UInteger = UInteger.valueOf(0),
    var executionId: Long = -1
)
