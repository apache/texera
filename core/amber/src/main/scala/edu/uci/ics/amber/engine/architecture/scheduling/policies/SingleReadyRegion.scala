package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion

class SingleReadyRegion(workflow: Workflow) extends SchedulingPolicy(workflow) {

  private var nextRegion: PipelinedRegion = null

  override def getNextRegionsToSchedule(): Set[PipelinedRegion] = {
    if (
      (nextRegion == null || completedRegions.contains(
        nextRegion
      )) && regionsScheduleOrderIterator.hasNext
    ) {
      nextRegion = regionsScheduleOrderIterator.next()
      return Set(nextRegion)
    }
    return Set()
  }
}
