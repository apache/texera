package edu.uci.ics.texera.workflow.common
import org.jooq.types.UInteger

class WorkflowContext(
    var jobId: String = null,
    var userId: Option[UInteger] = None,
    var wId: Int = -1,
    var executionID: Long = -1
)
