package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

class AllReadyRegions(scheduleOrder: mutable.Buffer[PipelinedRegion])
    extends SchedulingPolicy(scheduleOrder) {

  override def getNextSchedulingWork(workflow: Workflow): Set[PipelinedRegion] = {
    val nextToSchedule: mutable.HashSet[PipelinedRegion] = new mutable.HashSet[PipelinedRegion]()
    breakable {
      while (regionsScheduleOrder.nonEmpty) {
        val nextRegion = regionsScheduleOrder.head
        val upstreamRegions = workflow.executionPlan.regionAncestorMapping(nextRegion)
        if (upstreamRegions.forall(completedRegions.contains)) {
          assert(!scheduledRegions.contains(nextRegion))
          nextToSchedule.add(nextRegion)
          regionsScheduleOrder.remove(0)
          scheduledRegions.add(nextRegion)
        } else {
          break
        }
      }
    }

    nextToSchedule.toSet
  }
}
