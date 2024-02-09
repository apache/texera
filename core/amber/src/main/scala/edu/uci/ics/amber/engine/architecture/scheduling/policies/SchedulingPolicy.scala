package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.{ExecutionState, Workflow}
import edu.uci.ics.amber.engine.architecture.scheduling.{GlobalPortIdentity, Region, RegionIdentity}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink

import scala.collection.mutable

object SchedulingPolicy {
  def createPolicy(
      policyName: String,
      scheduleOrder: mutable.Buffer[Region]
  ): SchedulingPolicy = {
    if (policyName.equals("single-ready-region")) {
      new SingleReadyRegion(scheduleOrder)
    } else if (policyName.equals("all-ready-regions")) {
      new AllReadyRegions(scheduleOrder)
    } else {
      throw new WorkflowRuntimeException(s"Unknown scheduling policy name")
    }
  }
}

abstract class SchedulingPolicy(
    protected val regionsScheduleOrder: mutable.Buffer[Region]
) {

  // regions sent by the policy to be scheduled at least once
  protected val scheduledRegions = new mutable.HashSet[Region]()
  protected val completedRegions = new mutable.HashSet[Region]()
  // regions currently running
  protected val runningRegions = new mutable.HashSet[Region]()

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
  protected def getNextSchedulingWork(workflow: Workflow): Set[Region]

  def startWorkflow(workflow: Workflow): Set[Region] = {
    val regions = getNextSchedulingWork(workflow)
    if (regions.isEmpty) {
      throw new WorkflowRuntimeException(
        s"No first region is being scheduled"
      )
    }
    regions
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

  def removeFromRunningRegion(regions: Set[Region]): Unit = {
    runningRegions --= regions
  }

  def getRunningRegions: Set[Region] = runningRegions.toSet

  def getCompletedRegions: Set[Region] = completedRegions.toSet

}
