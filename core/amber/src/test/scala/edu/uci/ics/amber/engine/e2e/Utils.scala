package edu.uci.ics.amber.engine.e2e

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{
  BreakpointInfo,
  OperatorLink,
  WorkflowCompiler,
  LogicalPlan
}

object Utils {

  def getWorkflow(
      operators: List[OperatorDescriptor],
      links: List[OperatorLink],
      jobId: String = "workflow-test",
      workflowTag: String = "workflow-test"
  ): Workflow = {
    val context = new WorkflowContext
    context.jobId = jobId

    val texeraWorkflowCompiler = new WorkflowCompiler(
      LogicalPlan(operators, links, List[BreakpointInfo]()),
      context
    )

    texeraWorkflowCompiler.amberWorkflow(WorkflowIdentity(workflowTag), new OpResultStorage())
  }

}
