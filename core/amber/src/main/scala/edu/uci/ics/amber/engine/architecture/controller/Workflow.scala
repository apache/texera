package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.common.model.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.texera.workflow.common.workflow.LogicalPlan

case class Workflow(
    context: WorkflowContext,
    logicalPlan: LogicalPlan,
    physicalPlan: PhysicalPlan
)
