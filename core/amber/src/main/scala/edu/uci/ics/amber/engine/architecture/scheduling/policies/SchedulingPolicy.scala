package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.{ExecutionState, Workflow}
import edu.uci.ics.amber.engine.architecture.scheduling.Region
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState

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
    } else if (policyName.equals("single-ready-region-time-interleaved")) {
      new SingleReadyRegionTimeInterleaved(scheduleOrder)
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
  protected val completedLinksOfRegion: mutable.MultiDict[Region, PhysicalLink] =
    mutable.MultiDict[Region, PhysicalLink]()

  protected def isRegionCompleted(
      executionState: ExecutionState,
      region: Region
  ): Boolean = {

    region.downstreamLinks
      .subsetOf(
        completedLinksOfRegion.get(region)
      ) &&
    region.physicalOps
      .forall(operator =>
        executionState
          .getOperatorExecution(operator.id)
          .getState == WorkflowAggregatedState.COMPLETED
      )
  }

  protected def checkRegionCompleted(
      executionState: ExecutionState,
      region: Region
  ): Unit = {
    if (isRegionCompleted(executionState, region)) {
      runningRegions.remove(region)
      completedRegions.add(region)
    }
  }

  protected def getRegions(
      workflow: Workflow,
      workerId: ActorVirtualIdentity
  ): Set[Region] = {
    val operator = workflow.physicalPlan.getPhysicalOpByWorkerId(workerId)
    runningRegions.filter(r => r.physicalOps.contains(operator)).toSet
  }

  /**
    * A link's region is the region of the source operator of the link.
    */
  protected def getRegions(link: PhysicalLink): Set[Region] = {
    runningRegions.filter(r => r.physicalOps.map(_.id).contains(link.fromOpId)).toSet
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

  def onWorkerCompletion(
      workflow: Workflow,
      executionState: ExecutionState,
      workerId: ActorVirtualIdentity
  ): Set[Region] = {
    val regions = getRegions(workflow, workerId)
    regions.foreach(region => checkRegionCompleted(executionState, region))
    getNextSchedulingWork(workflow)
  }

  def onLinkCompletion(
      workflow: Workflow,
      executionState: ExecutionState,
      link: PhysicalLink
  ): Set[Region] = {
    val regions = getRegions(link)
    regions.foreach(region => completedLinksOfRegion.addOne((region, link)))
    regions.foreach(region => checkRegionCompleted(executionState, region))
    getNextSchedulingWork(workflow)
  }

  def onTimeSlotExpired(workflow: Workflow): Set[Region] = {
    getNextSchedulingWork(workflow)
  }

  def addToRunningRegions(regions: Set[Region], actorService: AkkaActorService): Unit = {
    runningRegions ++= regions
  }

  def removeFromRunningRegion(regions: Set[Region]): Unit = {
    runningRegions --= regions
  }

  def getRunningRegions: Set[Region] = runningRegions.toSet

  def getCompletedRegions: Set[Region] = completedRegions.toSet

}
