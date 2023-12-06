package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.{ExecutionState, Workflow}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RegionsTimeSlotExpiredHandler.RegionsTimeSlotExpired
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.util.control.Breaks.{break, breakable}

class SingleReadyRegionTimeInterleaved(scheduleOrder: mutable.Buffer[PipelinedRegion])
    extends SchedulingPolicy(scheduleOrder) {

  var currentlyExecutingRegions = new mutable.LinkedHashSet[PipelinedRegion]()

  override def checkRegionCompleted(
      workflow: Workflow,
      executionState: ExecutionState,
      region: PipelinedRegion
  ): Unit = {
    super.checkRegionCompleted(workflow, executionState, region)
    if (isRegionCompleted(workflow, executionState, region)) {
      currentlyExecutingRegions.remove(region)
    }
  }

  override def onWorkerCompletion(
      workflow: Workflow,
      executionState: ExecutionState,
      workerId: ActorVirtualIdentity
  ): Set[PipelinedRegion] = {
    val regions = getRegions(workflow, workerId)
    regions.foreach(r => checkRegionCompleted(workflow, executionState, r))
    if (regions.exists(r => isRegionCompleted(workflow, executionState, r))) {
      getNextSchedulingWork(workflow)
    } else {
      Set()
    }
  }

  override def onLinkCompletion(
      workflow: Workflow,
      executionState: ExecutionState,
      linkId: LinkIdentity
  ): Set[PipelinedRegion] = {
    val regions = getRegions(linkId)
    regions.foreach(r => completedLinksOfRegion.addBinding(r, linkId))
    regions.foreach(r => checkRegionCompleted(workflow, executionState, r))
    if (regions.exists(r => isRegionCompleted(workflow, executionState, r))) {
      getNextSchedulingWork(workflow)
    } else {
      Set()
    }
  }

  override def getNextSchedulingWork(workflow: Workflow): Set[PipelinedRegion] = {
    breakable {
      while (regionsScheduleOrder.nonEmpty) {
        val nextRegion = regionsScheduleOrder.head
        val upstreamRegions = workflow.physicalPlan.regionAncestorMapping(nextRegion)
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

  override def addToRunningRegions(
      regions: Set[PipelinedRegion],
      actorService: AkkaActorService
  ): Unit = {
    regions.foreach(r => runningRegions.add(r))
    actorService.sendToSelfOnce(
      FiniteDuration.apply(AmberConfig.timeSlotExpirationDurationInMs, MILLISECONDS),
      ControlInvocation(
        AsyncRPCClient.IgnoreReplyAndDoNotLog,
        RegionsTimeSlotExpired(regions)
      )
    )
  }
}
