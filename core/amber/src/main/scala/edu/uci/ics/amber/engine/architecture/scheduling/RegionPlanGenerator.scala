package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

abstract class RegionPlanGenerator(
    workflowContext: WorkflowContext,
    physicalPlan: PhysicalPlan,
    opResultStorage: OpResultStorage
) {

  def generate(): (RegionPlan, PhysicalPlan)

}
