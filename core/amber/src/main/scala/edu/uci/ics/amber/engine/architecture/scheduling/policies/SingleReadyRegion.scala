package edu.uci.ics.amber.engine.architecture.scheduling.policies

import akka.actor.ActorContext
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient

class SingleReadyRegion(workflow: Workflow, ctx: ActorContext, asyncRPCClient: AsyncRPCClient)
    extends SchedulingPolicy(workflow, ctx, asyncRPCClient) {

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
