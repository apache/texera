package edu.uci.ics.amber.engine.architecture.scheduling.policies

import akka.actor.ActorContext
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.mutable
import scala.collection.JavaConverters._

object SchedulingPolicy {
  def createPolicy(
      policyName: String,
      workflow: Workflow,
      ctx: ActorContext
  ): SchedulingPolicy = {
    if (policyName.equals("single-ready-region")) {
      new SingleReadyRegion(workflow)
    } else if (policyName.equals("all-ready-regions")) {
      new AllReadyRegions(workflow)
    } else if (policyName.equals("single-ready-region-time-interleaved")) {
      new SingleReadyRegionTimeInterleaved(workflow, ctx)
    } else {
      throw new WorkflowRuntimeException(s"Unknown scheduling policy name")
    }
  }
}

abstract class SchedulingPolicy(workflow: Workflow) {

  protected val regionsScheduleOrder: mutable.Buffer[PipelinedRegion] =
    new TopologicalOrderIterator(workflow.physicalPlan.pipelinedRegionsDAG).asScala.toBuffer

  // regions sent by the policy to be scheduled at least once
  protected val scheduledRegions = new mutable.HashSet[PipelinedRegion]()
  protected val completedRegions = new mutable.HashSet[PipelinedRegion]()
  // regions currently running
  protected val runningRegions = new mutable.HashSet[PipelinedRegion]()
  protected val completedLinksOfRegion =
    new mutable.HashMap[PipelinedRegion, mutable.Set[LinkIdentity]]
      with mutable.MultiMap[PipelinedRegion, LinkIdentity]

  protected def isRegionCompleted(region: PipelinedRegion): Boolean = {
    workflow
      .getBlockingOutLinksOfRegion(region)
      .subsetOf(completedLinksOfRegion.getOrElse(region, new mutable.HashSet[LinkIdentity]())) &&
    region
      .getOperators()
      .forall(opId => workflow.getOperator(opId).getState == WorkflowAggregatedState.COMPLETED)
  }

  protected def checkRegionCompleted(region: PipelinedRegion): Unit = {
    if (isRegionCompleted(region)) {
      runningRegions.remove(region)
      completedRegions.add(region)
    }
  }

  protected def getRegions(workerId: ActorVirtualIdentity): Set[PipelinedRegion] = {
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
  protected def getNextSchedulingWork(): Set[PipelinedRegion]

  def startWorkflow(): Set[PipelinedRegion] = {
    val regions = getNextSchedulingWork()
    if (regions.isEmpty) {
      throw new WorkflowRuntimeException(
        s"No first region is being scheduled"
      )
    }
    regions
  }

  def onWorkerCompletion(workerId: ActorVirtualIdentity): Set[PipelinedRegion] = {
    val regions = getRegions(workerId)
    regions.foreach(r => checkRegionCompleted(r))
    getNextSchedulingWork()
  }

  def onLinkCompletion(link: LinkIdentity): Set[PipelinedRegion] = {
    val regions = getRegions(link)
    regions.foreach(r => completedLinksOfRegion.addBinding(r, link))
    regions.foreach(r => checkRegionCompleted(r))
    getNextSchedulingWork()
  }

  def onTimeSlotExpired(): Set[PipelinedRegion] = {
    getNextSchedulingWork()
  }

  def addToRunningRegions(regions: Set[PipelinedRegion]): Unit = {
    runningRegions ++= regions
  }

  def removeFromRunningRegion(regions: Set[PipelinedRegion]): Unit = {
    runningRegions --= regions
  }

  def getRunningRegions(): Set[PipelinedRegion] = runningRegions.toSet

  def getCompletedRegions(): Set[PipelinedRegion] = completedRegions.toSet

}
