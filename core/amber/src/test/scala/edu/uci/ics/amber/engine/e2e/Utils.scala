package edu.uci.ics.amber.engine.e2e

import akka.actor.{Props}
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerEventListener}
import edu.uci.ics.amber.engine.common.ambertag.WorkflowTag
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
      workflowId: String = "workflow-test",
      workflowTag: String = "workflow-test"
  ): Props = {
    val context = new WorkflowContext
    context.workflowID = workflowId

    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(operators, links, mutable.MutableList[BreakpointInfo]()),
      context
    )
    texeraWorkflowCompiler.init()
    Controller.props(
      WorkflowTag.apply(workflowTag),
      texeraWorkflowCompiler.amberWorkflow,
      false,
      ControllerEventListener(),
      100
    )
  }

}
