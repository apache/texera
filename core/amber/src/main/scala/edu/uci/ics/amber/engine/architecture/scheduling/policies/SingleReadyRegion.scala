package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.Region

import scala.collection.mutable

class SingleReadyRegion(scheduleOrder: mutable.Buffer[Region])
    extends SchedulingPolicy(scheduleOrder) {

  override def getNextSchedulingWork(workflow: Workflow): Set[Region] = {
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
