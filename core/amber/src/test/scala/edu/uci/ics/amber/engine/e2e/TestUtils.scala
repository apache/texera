package edu.uci.ics.amber.engine.e2e

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{OperatorLink, WorkflowCompiler}

object TestUtils {

  def buildWorkflow(
      operators: List[OperatorDescriptor],
      links: List[OperatorLink],
      resultStorage: OpResultStorage = new OpResultStorage(),
      jobId: String = "workflow_test",
      workflowTag: String = "workflow_test"
  ): Workflow = {
    val context = new WorkflowContext
    context.jobId = jobId
    val workflowCompiler = new WorkflowCompiler(
      LogicalPlanPojo(operators, links, List(), List(), List()),
      context
    )
    workflowCompiler.compile(
      WorkflowIdentity(workflowTag),
      resultStorage,
      None,
      new JobStateStore()
    )
  }

}
