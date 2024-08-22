package edu.uci.ics.texera.workflow.common
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}

object WorkflowContext {
  val DEFAULT_EXECUTION_ID: ExecutionIdentity = ExecutionIdentity(1L)
  val DEFAULT_WORKFLOW_ID: WorkflowIdentity = WorkflowIdentity(1L)
}
class WorkflowContext(
    var workflowId: WorkflowIdentity = DEFAULT_WORKFLOW_ID,
    var executionId: ExecutionIdentity = DEFAULT_EXECUTION_ID
)
