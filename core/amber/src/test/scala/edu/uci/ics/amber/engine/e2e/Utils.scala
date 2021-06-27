package edu.uci.ics.amber.engine.e2e

import akka.actor.Props
import edu.uci.ics.amber.engine.architecture.controller.{
  Controller,
  ControllerConfig,
  ControllerEventListener
}
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{
  BreakpointInfo,
  OperatorLink,
  WorkflowCompiler,
  WorkflowInfo
}

import scala.collection.mutable

object Utils {

  def getControllerProps(
      operators: mutable.MutableList[OperatorDescriptor],
      links: mutable.MutableList[OperatorLink],
      jobId: String = "workflow-test",
      workflowTag: String = "workflow-test"
  ): Props = {
    val context = new WorkflowContext
    context.jobID = jobId

    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(operators, links, mutable.MutableList[BreakpointInfo]()),
      context
    )

    Controller.props(
      WorkflowIdentity(workflowTag),
      texeraWorkflowCompiler.amberWorkflow,
      ControllerEventListener(),
      ControllerConfig.default
    )
  }

}
