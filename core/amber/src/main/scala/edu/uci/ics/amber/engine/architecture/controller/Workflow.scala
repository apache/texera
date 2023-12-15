package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.scheduling.RegionPlan
import edu.uci.ics.amber.engine.common.virtualidentity._
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}

case class Workflow(
    executionId: ExecutionIdentity,
    originalLogicalPlan: LogicalPlan,
    logicalPlan: LogicalPlan,
    physicalPlan: PhysicalPlan,
    regionPlan: RegionPlan
) extends java.io.Serializable {}
