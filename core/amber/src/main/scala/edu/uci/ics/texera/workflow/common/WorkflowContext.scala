package edu.uci.ics.texera.workflow.common
import edu.uci.ics.texera.workflow.common.WorkflowContext.DEFAULT_EXECUTION_ID
import org.jooq.types.UInteger

object WorkflowContext {
  val DEFAULT_EXECUTION_ID = 1
}
class WorkflowContext(
    var jobId: String = "default",
    var userId: Option[UInteger] = None,
    var wid: UInteger = UInteger.valueOf(0),
    var executionId: Long = DEFAULT_EXECUTION_ID
)
