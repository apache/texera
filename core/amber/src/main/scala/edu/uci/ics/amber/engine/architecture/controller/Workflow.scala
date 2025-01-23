package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.texera.workflow.LogicalPlan

case class Workflow(
    context: WorkflowContext,
    logicalPlan: LogicalPlan,
    physicalPlan: PhysicalPlan
)
