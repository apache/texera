package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.scheduling.{
  ExpansionGreedyRegionPlanGenerator,
  Region,
  RegionPlan,
  Schedule
}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

class WorkflowScheduler(workflowContext: WorkflowContext, opResultStorage: OpResultStorage)
    extends java.io.Serializable {
  var physicalPlan: PhysicalPlan = _
  var regionPlan: RegionPlan = _
  private var schedule: Schedule = _

  /**
    * Update the schedule to be executed, based on the current physicalPlan.
    */
  def updateSchedule(physicalPlan: PhysicalPlan): Unit = {
    // generate an RegionPlan with regions.
    //  currently, ExpansionGreedyRegionPlanGenerator is the only RegionPlan generator.
    val (regionPlan, updatedPhysicalPlan) = new ExpansionGreedyRegionPlanGenerator(
      workflowContext,
      physicalPlan,
      opResultStorage
    ).generate()
    this.regionPlan = regionPlan
    this.physicalPlan = updatedPhysicalPlan
    this.schedule = Schedule.apply(regionPlan)
  }

  def getNextRegions: Set[Region] = schedule.next()

}
