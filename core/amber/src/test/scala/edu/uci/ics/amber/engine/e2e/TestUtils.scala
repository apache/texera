package edu.uci.ics.amber.engine.e2e

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{LogicalLink, WorkflowCompiler}

object TestUtils {

  def buildWorkflow(
      operators: List[LogicalOp],
      links: List[LogicalLink],
      resultStorage: OpResultStorage
  ): Workflow = {
    val context = new WorkflowContext()
    val workflowCompiler = new WorkflowCompiler(
      context
    )
    workflowCompiler.compile(
      LogicalPlanPojo(operators, links, List(), List()),
      resultStorage,
      new ExecutionStateStore()
    )
  }

}
