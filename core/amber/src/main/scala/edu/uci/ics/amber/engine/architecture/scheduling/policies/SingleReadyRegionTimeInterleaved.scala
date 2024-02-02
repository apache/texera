package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.{ExecutionState, Workflow}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RegionsTimeSlotExpiredHandler.RegionsTimeSlotExpired
import edu.uci.ics.amber.engine.architecture.scheduling.Region
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink

import scala.collection.mutable
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
import scala.util.control.Breaks.{break, breakable}

class SingleReadyRegionTimeInterleaved(scheduleOrder: mutable.Buffer[Region])
    extends SchedulingPolicy(scheduleOrder) {

  var currentlyExecutingRegions = new mutable.LinkedHashSet[Region]()

  override def checkRegionCompleted(
      executionState: ExecutionState,
      region: Region
  ): Unit = {
    super.checkRegionCompleted(executionState, region)
    if (isRegionCompleted(executionState, region)) {
      currentlyExecutingRegions.remove(region)
    }
  }

  override def onWorkerCompletion(
      workflow: Workflow,
      executionState: ExecutionState,
      workerId: ActorVirtualIdentity
  ): Set[Region] = {
    val regions = getRegions(workflow, workerId)
    regions.foreach(region => checkRegionCompleted(executionState, region))
    if (regions.exists(region => isRegionCompleted(executionState, region))) {
      getNextSchedulingWork(workflow)
    } else {
      Set()
    }
  }

  override def onLinkCompletion(
      workflow: Workflow,
      executionState: ExecutionState,
      link: PhysicalLink
  ): Set[Region] = {
    val regions = getRegions(link)
    regions.foreach(region => completedLinksOfRegion.addBinding(region, link))
    regions.foreach(region => checkRegionCompleted(executionState, region))
    if (regions.exists(region => isRegionCompleted(executionState, region))) {
      getNextSchedulingWork(workflow)
    } else {
      Set()
    }
  }

  override def getNextSchedulingWork(workflow: Workflow): Set[Region] = {
    breakable {
      while (regionsScheduleOrder.nonEmpty) {
        val nextRegion = regionsScheduleOrder.head
        val upstreamRegions = workflow.regionPlan.getUpstreamRegions(nextRegion)
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
      regions: Set[Region],
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
