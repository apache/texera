package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.{ExecutionState, Workflow}
import edu.uci.ics.amber.engine.architecture.scheduling.{GlobalPortIdentity, Region, RegionIdentity}

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}



class SchedulingPolicy(
    protected val regionsScheduleOrder: mutable.Buffer[Region]
) {

  // regions sent by the policy to be scheduled at least once
  protected val scheduledRegions = new mutable.HashSet[Region]()
  protected val completedRegions = new mutable.HashSet[Region]()
  // regions currently running
  private val runningRegions = new mutable.HashSet[Region]()

  private val completedPortIdsOfRegion
      : mutable.HashMap[RegionIdentity, mutable.HashSet[GlobalPortIdentity]] = mutable.HashMap()
  private def isRegionCompleted(
      executionState: ExecutionState,
      region: Region
  ): Boolean = {
    region.getPorts.subsetOf(completedPortIdsOfRegion.getOrElse(region.id, mutable.HashSet()))
  }

  private def getRegion(portId: GlobalPortIdentity): Option[Region] = {
    runningRegions.find(r => r.getPorts.contains(portId))
  }

  // gets the ready regions that is not currently running
  protected def getNextSchedulingWork(workflow: Workflow): Set[Region] = {
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

  def startWorkflow(workflow: Workflow): Set[Region] = {
    getNextSchedulingWork(workflow)
  }

  def onPortCompletion(
      workflow: Workflow,
      executionState: ExecutionState,
      portId: GlobalPortIdentity
  ): Set[Region] = {
    getRegion(portId) match {
      case Some(region) =>
        val portIds =
          completedPortIdsOfRegion.getOrElseUpdate(
            region.id,
            new mutable.HashSet[GlobalPortIdentity]()
          )
        portIds.add(portId)
        if (isRegionCompleted(executionState, region)) {
          runningRegions.remove(region)
          completedRegions.add(region)
        }
        getNextSchedulingWork(workflow)
      case None =>
        Set() // currently, the virtual input ports of source operators do not belong to any region
    }
  }

  def addToRunningRegions(regions: Set[Region]): Unit = {
    runningRegions ++= regions
  }

  def getCompletedRegions: Set[Region] = completedRegions.toSet

}
