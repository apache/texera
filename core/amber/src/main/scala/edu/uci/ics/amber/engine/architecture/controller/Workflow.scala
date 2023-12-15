package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.scheduling.RegionPlan
import edu.uci.ics.amber.engine.common.virtualidentity._
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}

class Workflow(
    val workflowId: WorkflowIdentity,
    val originalLogicalPlan: LogicalPlan,
    val logicalPlan: LogicalPlan,
    val physicalPlan: PhysicalPlan,
    val regionPlan: RegionPlan
) extends java.io.Serializable {}
