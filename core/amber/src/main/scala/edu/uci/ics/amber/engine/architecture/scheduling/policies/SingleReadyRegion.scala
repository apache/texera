package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion

import scala.collection.mutable

class SingleReadyRegion(scheduleOrder: mutable.Buffer[PipelinedRegion])
    extends SchedulingPolicy(scheduleOrder) {

  override def getNextSchedulingWork(workflow: Workflow): Set[PipelinedRegion] = {
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
