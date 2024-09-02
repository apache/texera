package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}

case class Workflow(
    context: WorkflowContext,
    logicalPlan: LogicalPlan,
    physicalPlan: PhysicalPlan
)
