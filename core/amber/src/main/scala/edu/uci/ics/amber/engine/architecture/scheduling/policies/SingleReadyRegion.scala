package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion

class SingleReadyRegion(workflow: Workflow) extends SchedulingPolicy(workflow) {

  override def getNextSchedulingWork(): Set[PipelinedRegion] = {
    if (
      (scheduledRegions.isEmpty ||
      scheduledRegions.forall(completedRegions.contains)) && regionsScheduleOrder.nonEmpty
    ) {
      val nextRegion = regionsScheduleOrder.head
      regionsScheduleOrder.remove(0)
      assert(!scheduledRegions.contains(nextRegion))
      scheduledRegions.add(nextRegion)
      return Set(nextRegion)
    }
    Set()
  }
}
