package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.Region

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

class AllReadyRegions(scheduleOrder: mutable.Buffer[Region])
    extends SchedulingPolicy(scheduleOrder) {

  override def getNextSchedulingWork(workflow: Workflow): Set[Region] = {
    val nextToSchedule: mutable.HashSet[Region] = new mutable.HashSet[Region]()
    breakable {
      while (regionsScheduleOrder.nonEmpty) {
        val nextRegion = regionsScheduleOrder.head
        val upstreamRegions = workflow.regionPlan.getUpstreamRegions(nextRegion)
        if (upstreamRegions.forall(completedRegions.contains)) {
          assert(!scheduledRegions.contains(nextRegion))
          nextToSchedule.add(nextRegion)
          regionsScheduleOrder.remove(0)
          scheduledRegions.add(nextRegion)
        } else {
          break()
        }
      }
    }

    nextToSchedule.toSet
  }
}
