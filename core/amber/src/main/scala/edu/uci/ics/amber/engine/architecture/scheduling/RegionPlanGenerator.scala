package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}

abstract class RegionPlanGenerator(
    physicalPlan: PhysicalPlan,
    opResultStorage: OpResultStorage
) {

  def generate(context: WorkflowContext): (RegionPlan, PhysicalPlan)

}
