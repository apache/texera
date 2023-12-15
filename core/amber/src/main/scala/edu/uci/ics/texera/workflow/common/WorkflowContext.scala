package edu.uci.ics.texera.workflow.common
import edu.uci.ics.amber.engine.common.virtualidentity.ExecutionIdentity
import edu.uci.ics.texera.workflow.common.WorkflowContext.DEFAULT_EXECUTION_ID
import org.jooq.types.UInteger

object WorkflowContext {
  val DEFAULT_EXECUTION_ID = ExecutionIdentity(1L)
}
class WorkflowContext(
    var userId: Option[UInteger] = None,
    var wid: UInteger = UInteger.valueOf(0),
    var executionId: ExecutionIdentity = DEFAULT_EXECUTION_ID
)
