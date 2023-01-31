package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion

import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaSet
import scala.util.control.Breaks.{break, breakable}

class AllReadyRegions(workflow: Workflow) extends SchedulingPolicy(workflow) {

  override def getNextSchedulingWork(): Set[PipelinedRegion] = {
    val nextToSchedule: mutable.HashSet[PipelinedRegion] = new mutable.HashSet[PipelinedRegion]()
    breakable {
      while (regionsScheduleOrder.nonEmpty) {
        val nextRegion = regionsScheduleOrder.head
        val upstreamRegions =
          asScalaSet(workflow.physicalPlan.pipelinedRegionsDAG.getAncestors(nextRegion))
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
