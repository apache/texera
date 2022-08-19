package edu.uci.ics.amber.engine.architecture.scheduling.policies

import akka.actor.ActorContext
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RegionsTimeSlotExpiredHandler.RegionsTimeSlotExpired
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.jdk.CollectionConverters.asScalaSet
import scala.util.control.Breaks.{break, breakable}

class AllReadyTimeInterleavedRegions(
    workflow: Workflow,
    ctx: ActorContext,
    asyncRPCClient: AsyncRPCClient
) extends SchedulingPolicy(workflow, ctx, asyncRPCClient) {

  var currentlyExecutingRegions = new mutable.LinkedHashSet[PipelinedRegion]()

  override def checkRegionCompleted(region: PipelinedRegion): Unit = {
    val isRegionCompleted: Boolean = workflow
      .getBlockingOutlinksOfRegion(region)
      .forall(
        completedLinksOfRegion.getOrElse(region, new mutable.HashSet[LinkIdentity]()).contains
      ) && region
      .getOperators()
      .forall(opId => workflow.getOperator(opId).getState == WorkflowAggregatedState.COMPLETED)
    if (isRegionCompleted) {
      runningRegions.remove(region)
      currentlyExecutingRegions.remove(region)
      completedRegions.add(region)
    }
  }

  override def getNextSchedulingWork(): Set[PipelinedRegion] = {
    breakable {
      while (regionsScheduleOrder.nonEmpty) {
        val nextRegion = regionsScheduleOrder.head
        val upstreamRegions = asScalaSet(workflow.getPipelinedRegionsDAG().getAncestors(nextRegion))
        if (upstreamRegions.forall(completedRegions.contains)) {
          assert(!sentToBeScheduledRegions.contains(nextRegion))
          currentlyExecutingRegions.add(nextRegion)
          regionsScheduleOrder.remove(0)
          sentToBeScheduledRegions.add(nextRegion)
        } else {
          break
        }
      }
    }
    val nextToSchedule = currentlyExecutingRegions.head
    currentlyExecutingRegions.remove(nextToSchedule)
    currentlyExecutingRegions.add(nextToSchedule)
    ctx.system.scheduler.scheduleOnce(
      FiniteDuration.apply(5000, MILLISECONDS),
      ctx.self,
      ControlInvocation(
        AsyncRPCClient.IgnoreReplyAndDoNotLog,
        RegionsTimeSlotExpired(Set(nextToSchedule))
      )
    )(ctx.dispatcher)
    Set(nextToSchedule)
  }
}
