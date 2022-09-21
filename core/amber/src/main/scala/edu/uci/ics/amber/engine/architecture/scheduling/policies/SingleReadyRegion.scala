package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion

class SingleReadyRegion(workflow: Workflow) extends SchedulingPolicy(workflow) {

  override def getNextSchedulingWork(): Set[PipelinedRegion] = {
    if (
      (sentToBeScheduledRegions.isEmpty || sentToBeScheduledRegions.forall(
        completedRegions.contains
      )) && regionsScheduleOrder.nonEmpty
    ) {
      val nextRegion = regionsScheduleOrder.head
      regionsScheduleOrder.remove(0)
      assert(!sentToBeScheduledRegions.contains(nextRegion))
      sentToBeScheduledRegions.add(nextRegion)
      return Set(nextRegion)
    }
    Set()
  }
}
