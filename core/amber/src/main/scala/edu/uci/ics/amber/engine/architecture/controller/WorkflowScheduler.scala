package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.core.storage.result.OpResultStorage
import edu.uci.ics.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.engine.architecture.scheduling.{
  CostBasedRegionPlanGenerator,
  ExpansionGreedyRegionPlanGenerator,
  Region,
  Schedule
}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.virtualidentity.ActorVirtualIdentity

class WorkflowScheduler(
    workflowContext: WorkflowContext,
    opResultStorage: OpResultStorage,
    actorId: ActorVirtualIdentity
) extends java.io.Serializable {
  var physicalPlan: PhysicalPlan = _
  private var schedule: Schedule = _

  /**
    * Update the schedule to be executed, based on the given physicalPlan.
    */
  def updateSchedule(physicalPlan: PhysicalPlan): Unit = {
    // generate an RegionPlan with regions using a region plan generator.
    val (regionPlan, updatedPhysicalPlan) = if (AmberConfig.enableCostBasedRegionPlanGenerator) {
      // CostBasedRegionPlanGenerator considers costs to try to find an optimal plan.
      new CostBasedRegionPlanGenerator(
        workflowContext,
        physicalPlan,
        opResultStorage,
        actorId
      ).generate()
    } else {
      // ExpansionGreedyRegionPlanGenerator is the stable default plan generator.
      new ExpansionGreedyRegionPlanGenerator(
        workflowContext,
        physicalPlan,
        opResultStorage
      ).generate()
    }
    this.physicalPlan = updatedPhysicalPlan
    this.schedule = Schedule.apply(regionPlan)
  }

  def getNextRegions: Set[Region] = if (!schedule.hasNext) Set() else schedule.next()

}
