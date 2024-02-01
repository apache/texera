package edu.uci.ics.amber.engine.architecture.scheduling

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}

class CostBasedRegionPlanGenerator(
    logicalPlan: LogicalPlan,
    var physicalPlan: PhysicalPlan,
    opResultStorage: OpResultStorage
) extends RegionPlanGenerator(
      logicalPlan,
      physicalPlan,
      opResultStorage
    ) with LazyLogging {
  override def generate(context: WorkflowContext): (RegionPlan, PhysicalPlan) = ???

  // TODO: schedulability testing

}
