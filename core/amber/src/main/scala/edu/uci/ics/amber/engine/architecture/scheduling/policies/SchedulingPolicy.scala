package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.{PipelinedRegion, SchedulingWork}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SchedulingPolicy {
  def createPolicy(policyName: String, workflow: Workflow): SchedulingPolicy = {
    if (policyName.equals("single-ready-region")) {
      new SingleReadyRegion(workflow)
    } else if (policyName.equals("all-ready-regions")) {
      new AllReadyRegions(workflow)
    } else if (policyName.equals("all-ready-time-interleaved-regions")) {
      new AllReadyTimeInterleavedRegions(workflow)
    } else {
      throw new WorkflowRuntimeException(s"Unknown scheduling policy name")
    }
  }
}

abstract class SchedulingPolicy(workflow: Workflow) {

  protected val regionsScheduleOrder = saveTopologicalOrderOfRegions()

  var sentToBeScheduledRegions =
    new mutable.HashSet[
      PipelinedRegion
    ]() // regions sent by the policy to be scheduled at least once
  var completedRegions = new mutable.HashSet[PipelinedRegion]()
  var runningRegions = new mutable.HashSet[PipelinedRegion]() // regions currently running
  var completedLinksOfRegion =
    new mutable.HashMap[PipelinedRegion, mutable.HashSet[LinkIdentity]]()

  private def saveTopologicalOrderOfRegions(): ArrayBuffer[PipelinedRegion] = {
    val scheduleOrder = new ArrayBuffer[PipelinedRegion]
    val regionsScheduleOrderIterator =
      new TopologicalOrderIterator[PipelinedRegion, DefaultEdge](workflow.getPipelinedRegionsDAG())
    while (regionsScheduleOrderIterator.hasNext()) {
      scheduleOrder.append(regionsScheduleOrderIterator.next())
    }
    scheduleOrder
  }

  protected def checkRegionCompleted(region: PipelinedRegion): Unit = {
    val isRegionCompleted: Boolean = workflow
      .getBlockingOutlinksOfRegion(region)
      .forall(
        completedLinksOfRegion.getOrElse(region, new mutable.HashSet[LinkIdentity]()).contains
      ) && region
      .getOperators()
      .forall(opId => workflow.getOperator(opId).getState == WorkflowAggregatedState.COMPLETED)
    if (isRegionCompleted) {
      runningRegions.remove(region)
      completedRegions.add(region)
    }
  }

  private def getRegion(workerId: ActorVirtualIdentity): Option[PipelinedRegion] = {
    val opId = workflow.getOperator(workerId).id
    var region: Option[PipelinedRegion] = None
    runningRegions.foreach(r =>
      if (r.getOperators().contains(opId)) {
        region = Some(r)
      }
    )
    region
  }

  /**
    * A link's region is the region of the source operator of the link.
    *
    * @param linkId
    * @return
    */
  private def getRegion(linkId: LinkIdentity): Option[PipelinedRegion] = {
    val upstreamOpId = OperatorIdentity(linkId.from.workflow, linkId.from.operator)
    var region: Option[PipelinedRegion] = None
    runningRegions.foreach(r =>
      if (r.getOperators().contains(upstreamOpId)) {
        region = Some(r)
      }
    )
    region
  }

  // gets the ready regions that haven't been sent for scheduling yet (not in `sentToBeScheduledRegions`)
  protected def getNextSchedulingWork(): SchedulingWork

  def recordWorkerCompletion(workerId: ActorVirtualIdentity): SchedulingWork = {
    val region = getRegion(workerId)
    if (region.isEmpty) {
      throw new WorkflowRuntimeException(
        s"WorkflowScheduler: Worker ${workerId} completed from a non-running region"
      )
    } else {
      checkRegionCompleted(region.get)
    }
    getNextSchedulingWork()
  }

  def recordLinkCompletion(linkId: LinkIdentity): SchedulingWork = {
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
    getNextSchedulingWork()
  }

  def startWorkflow(): SchedulingWork = {
    val work = getNextSchedulingWork()
    if (work.regions.isEmpty) {
      throw new WorkflowRuntimeException(
        s"No first region is being scheduled"
      )
    }
    work
  }

  def recordTimeFinished(regions: Set[PipelinedRegion]): SchedulingWork = {
    getNextSchedulingWork()
  }
}
