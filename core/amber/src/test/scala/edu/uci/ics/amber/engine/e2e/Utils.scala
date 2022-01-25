package edu.uci.ics.amber.engine.e2e

import akka.actor.Props
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{
  BreakpointInfo,
  OperatorLink,
  WorkflowCompiler,
  WorkflowInfo
}

import scala.collection.mutable

object Utils {

  def getWorkflow(
      operators: mutable.MutableList[OperatorDescriptor],
      links: mutable.MutableList[OperatorLink],
      jobId: String = "workflow-test",
      workflowTag: String = "workflow-test"
  ): Workflow = {
    val context = new WorkflowContext
    context.jobId = jobId

    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(operators, links, mutable.MutableList[BreakpointInfo]()),
      context
    )

    texeraWorkflowCompiler.amberWorkflow(WorkflowIdentity(workflowTag), new OpResultStorage())
  }

}
