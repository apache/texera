package edu.uci.ics.amber.core.workflow

import edu.uci.ics.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID,
  DEFAULT_WORKFLOW_SETTINGS
}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}

object WorkflowContext {
  val DEFAULT_EXECUTION_ID: ExecutionIdentity = ExecutionIdentity(1L)
  val DEFAULT_WORKFLOW_ID: WorkflowIdentity = WorkflowIdentity(1L)
  val DEFAULT_WORKFLOW_SETTINGS: WorkflowSettings = WorkflowSettings(
    400 // TODO: make this configurable
  )
}
class WorkflowContext(
    var workflowId: WorkflowIdentity = DEFAULT_WORKFLOW_ID,
    var executionId: ExecutionIdentity = DEFAULT_EXECUTION_ID,
    var workflowSettings: WorkflowSettings = DEFAULT_WORKFLOW_SETTINGS
)
