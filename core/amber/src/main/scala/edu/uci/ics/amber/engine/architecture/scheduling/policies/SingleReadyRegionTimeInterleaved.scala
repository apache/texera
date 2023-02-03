package edu.uci.ics.amber.engine.architecture.scheduling.policies

import akka.actor.ActorContext
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RegionsTimeSlotExpiredHandler.RegionsTimeSlotExpired
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.jdk.CollectionConverters.asScalaSet
import scala.util.control.Breaks.{break, breakable}

class SingleReadyRegionTimeInterleaved(
    workflow: Workflow,
    ctx: ActorContext
) extends SchedulingPolicy(workflow) {

  var currentlyExecutingRegions = new mutable.LinkedHashSet[PipelinedRegion]()

  override def checkRegionCompleted(region: PipelinedRegion): Unit = {
    super.checkRegionCompleted(region)
    if (isRegionCompleted(region)) {
      currentlyExecutingRegions.remove(region)
    }
  }

  override def onWorkerCompletion(workerId: ActorVirtualIdentity): Set[PipelinedRegion] = {
    val region = getRegion(workerId)
    if (region.isEmpty) {
      throw new WorkflowRuntimeException(
        s"WorkflowScheduler: Worker ${workerId} completed from a non-running region"
      )
    } else {
      checkRegionCompleted(region.get)
    }
    if (isRegionCompleted(region.get)) {
      getNextSchedulingWork()
    } else {
      Set()
    }
  }

  override def onLinkCompletion(linkId: LinkIdentity): Set[PipelinedRegion] = {
    val region = getRegion(linkId)
    if (region == null) {
      throw new WorkflowRuntimeException(
        s"WorkflowScheduler: Link ${linkId.toString()} completed from a non-running region"
      )
    } else {
      val completedLinks =
        completedLinksOfRegion.getOrElseUpdate(region.get, new mutable.HashSet[LinkIdentity]())
      completedLinks.add(linkId)
      completedLinksOfRegion(region.get) = completedLinks
      checkRegionCompleted(region.get)
    }
    if (isRegionCompleted(region.get)) {
      getNextSchedulingWork()
    } else {
      Set()
    }
  }

  override def getNextSchedulingWork(): Set[PipelinedRegion] = {
    breakable {
      while (regionsScheduleOrder.nonEmpty) {
        val nextRegion = regionsScheduleOrder.head
        val upstreamRegions =
          asScalaSet(workflow.physicalPlan.pipelinedRegionsDAG.getAncestors(nextRegion))
        if (upstreamRegions.forall(completedRegions.contains)) {
          assert(!scheduledRegions.contains(nextRegion))
          currentlyExecutingRegions.add(nextRegion)
          regionsScheduleOrder.remove(0)
          scheduledRegions.add(nextRegion)
        } else {
          break
        }
      }
    }
    if (currentlyExecutingRegions.nonEmpty) {
      val nextToSchedule = currentlyExecutingRegions.head
      if (!runningRegions.contains(nextToSchedule)) {
        // if `nextToSchedule` is not running right now.
        currentlyExecutingRegions.remove(nextToSchedule) // remove first element
        currentlyExecutingRegions.add(nextToSchedule) // add to end of list
        return Set(nextToSchedule)
      }
    }
    Set()

  }

  override def addToRunningRegions(regions: Set[PipelinedRegion]): Unit = {
    regions.foreach(r => runningRegions.add(r))
    ctx.system.scheduler.scheduleOnce(
      FiniteDuration.apply(Constants.timeSlotExpirationDurationInMs, MILLISECONDS),
      ctx.self,
      ControlInvocation(
        AsyncRPCClient.IgnoreReplyAndDoNotLog,
        RegionsTimeSlotExpired(regions)
      )
    )(ctx.dispatcher)
  }
}
