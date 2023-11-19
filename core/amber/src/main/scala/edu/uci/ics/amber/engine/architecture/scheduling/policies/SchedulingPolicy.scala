package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.{ExecutionState, Workflow}
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState

import scala.collection.mutable

object SchedulingPolicy {
  def createPolicy(
      policyName: String,
      scheduleOrder: mutable.Buffer[PipelinedRegion]
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
    protected val regionsScheduleOrder: mutable.Buffer[PipelinedRegion]
) {

  // regions sent by the policy to be scheduled at least once
  protected val scheduledRegions = new mutable.HashSet[PipelinedRegion]()
  protected val completedRegions = new mutable.HashSet[PipelinedRegion]()
  // regions currently running
  protected val runningRegions = new mutable.HashSet[PipelinedRegion]()
  protected val completedLinksOfRegion =
    new mutable.HashMap[PipelinedRegion, mutable.Set[LinkIdentity]]
      with mutable.MultiMap[PipelinedRegion, LinkIdentity]

  protected def isRegionCompleted(
      workflow: Workflow,
      executionState: ExecutionState,
      region: PipelinedRegion
  ): Boolean = {
    workflow
      .getBlockingOutLinksOfRegion(region)
      .subsetOf(completedLinksOfRegion.getOrElse(region, new mutable.HashSet[LinkIdentity]())) &&
    region
      .getOperators()
      .forall(opId =>
        executionState.getOperatorExecution(opId).getState == WorkflowAggregatedState.COMPLETED
      )
  }

  protected def checkRegionCompleted(
      workflow: Workflow,
      executionState: ExecutionState,
      region: PipelinedRegion
  ): Unit = {
    if (isRegionCompleted(workflow, executionState, region)) {
      runningRegions.remove(region)
      completedRegions.add(region)
    }
  }

  protected def getRegions(
      workflow: Workflow,
      workerId: ActorVirtualIdentity
  ): Set[PipelinedRegion] = {
    val opId = workflow.getOperator(workerId).id
    runningRegions.filter(r => r.getOperators().contains(opId)).toSet
  }

  /**
    * A link's region is the region of the source operator of the link.
    */
  protected def getRegions(link: LinkIdentity): Set[PipelinedRegion] = {
    runningRegions.filter(r => r.getOperators().contains(link.from)).toSet
  }

  // gets the ready regions that is not currently running
  protected def getNextSchedulingWork(workflow: Workflow): Set[PipelinedRegion]

  def startWorkflow(workflow: Workflow): Set[PipelinedRegion] = {
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
  ): Set[PipelinedRegion] = {
    val regions = getRegions(workflow, workerId)
    regions.foreach(r => checkRegionCompleted(workflow, executionState, r))
    getNextSchedulingWork(workflow)
  }

  def onLinkCompletion(
      workflow: Workflow,
      executionState: ExecutionState,
      link: LinkIdentity
  ): Set[PipelinedRegion] = {
    val regions = getRegions(link)
    regions.foreach(r => completedLinksOfRegion.addBinding(r, link))
    regions.foreach(r => checkRegionCompleted(workflow, executionState, r))
    getNextSchedulingWork(workflow)
  }

  def onTimeSlotExpired(workflow: Workflow): Set[PipelinedRegion] = {
    getNextSchedulingWork(workflow)
  }

  def addToRunningRegions(regions: Set[PipelinedRegion], actorService: AkkaActorService): Unit = {
    runningRegions ++= regions
  }

  def removeFromRunningRegion(regions: Set[PipelinedRegion]): Unit = {
    runningRegions --= regions
  }

  def getRunningRegions(): Set[PipelinedRegion] = runningRegions.toSet

  def getCompletedRegions(): Set[PipelinedRegion] = completedRegions.toSet

}
