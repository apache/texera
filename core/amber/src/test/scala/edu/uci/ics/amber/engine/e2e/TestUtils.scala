package edu.uci.ics.amber.engine.e2e

import edu.uci.ics.amber.core.storage.result.OpResultStorage
import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.workflow.{LogicalLink, WorkflowCompiler}

object TestUtils {

  def buildWorkflow(
      operators: List[LogicalOp],
      links: List[LogicalLink],
      resultStorage: OpResultStorage,
      context: WorkflowContext
  ): Workflow = {
    val workflowCompiler = new WorkflowCompiler(
      context
    )
    workflowCompiler.compile(
      LogicalPlanPojo(operators, links, List(), List()),
      resultStorage
    )
  }

}
