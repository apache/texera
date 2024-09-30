package edu.uci.ics.texera.workflow.common
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID,
  DEFAULT_WORKFLOW_SETTINGS
}

import edu.uci.ics.texera.workflow.common.workflow.WorkflowSettings

object WorkflowContext {
  val DEFAULT_EXECUTION_ID: ExecutionIdentity = ExecutionIdentity(1L)
  val DEFAULT_WORKFLOW_ID: WorkflowIdentity = WorkflowIdentity(1L)
  val DEFAULT_WORKFLOW_SETTINGS: WorkflowSettings = WorkflowSettings(
    AmberConfig.defaultDataTransferBatchSize
  )
}
class WorkflowContext(
    var workflowId: WorkflowIdentity = DEFAULT_WORKFLOW_ID,
    var executionId: ExecutionIdentity = DEFAULT_EXECUTION_ID,
    var workflowSettings: WorkflowSettings = DEFAULT_WORKFLOW_SETTINGS
)
