package edu.uci.ics.amber.engine.e2e

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{LogicalLink, WorkflowCompiler}

object TestUtils {

  def buildWorkflow(
      operators: List[LogicalOp],
      links: List[LogicalLink],
      resultStorage: OpResultStorage = new OpResultStorage(),
      jobId: String = "workflow_test"
  ): Workflow = {
    val context = new WorkflowContext
    context.jobId = jobId
    val workflowCompiler = new WorkflowCompiler(
      LogicalPlanPojo(operators, links, List(), List(), List()),
      context
    )
    val workflowIdentity = WorkflowIdentity(context.executionId)
    workflowCompiler.compile(
      workflowIdentity,
      resultStorage,
      None,
      new JobStateStore()
    )
  }

}
