package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}

abstract class ExecutionPlanGenerator(
    workflowId: WorkflowIdentity,
    workflowContext: WorkflowContext,
    logicalPlan: LogicalPlan,
    physicalPlan: PhysicalPlan,
    opResultStorage: OpResultStorage
) {

  def generate(): (ExecutionPlan, PhysicalPlan)

}
