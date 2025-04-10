package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.engine.architecture.scheduling.{
  CostBasedScheduleGenerator,
  ExpansionGreedyScheduleGenerator,
  Region,
  Schedule
}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

class WorkflowScheduler(
    workflowContext: WorkflowContext,
    actorId: ActorVirtualIdentity
) extends java.io.Serializable {
  var physicalPlan: PhysicalPlan = _
  private var schedule: Schedule = _

  /**
    * Update the schedule to be executed, based on the given physicalPlan.
    */
  def updateSchedule(physicalPlan: PhysicalPlan): Unit = {
    // generate a schedule using a region plan generator.
    val (generatedSchedule, updatedPhysicalPlan) =
      if (AmberConfig.enableCostBasedScheduleGenerator) {
        // CostBasedRegionPlanGenerator considers costs to try to find an optimal plan.
        new CostBasedScheduleGenerator(
          workflowContext,
          physicalPlan,
          actorId
        ).generate()
      } else {
        // ExpansionGreedyRegionPlanGenerator is the stable default plan generator.
        new ExpansionGreedyScheduleGenerator(
          workflowContext,
          physicalPlan
        ).generate()
      }
    this.schedule = generatedSchedule
    this.physicalPlan = updatedPhysicalPlan
  }

  def getNextRegions: Set[Region] = if (!schedule.hasNext) Set() else schedule.next()

}
