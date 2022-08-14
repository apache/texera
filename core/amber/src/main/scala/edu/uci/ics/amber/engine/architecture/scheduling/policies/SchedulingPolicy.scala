package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
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

abstract class SchedulingPolicy(workflow: Workflow) {

  protected val regionsScheduleOrderIterator =
    new TopologicalOrderIterator[PipelinedRegion, DefaultEdge](workflow.getPipelinedRegionsDAG())
  var completedRegions = new mutable.HashSet[PipelinedRegion]()
  var runningRegions = new mutable.HashSet[PipelinedRegion]()
  var completedLinksOfRegion =
    new mutable.HashMap[PipelinedRegion, mutable.HashSet[LinkIdentity]]()

  private def isRegionCompleted(region: PipelinedRegion): Boolean = {
    workflow
      .getBlockingOutlinksOfRegion(region)
      .forall(
        completedLinksOfRegion.getOrElse(region, new mutable.HashSet[LinkIdentity]()).contains
      ) && region
      .getOperators()
      .forall(opId => workflow.getOperator(opId).getState == WorkflowAggregatedState.COMPLETED)
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

  def recordWorkerCompletion(workerId: ActorVirtualIdentity): Boolean = {
    val region = getRegion(workerId)
    if (region.isEmpty) {
      throw new WorkflowRuntimeException(
        s"WorkflowScheduler: Worker ${workerId} completed from a non-running region"
      )
    } else {
      if (isRegionCompleted(region.get)) {
        runningRegions.remove(region.get)
        completedRegions.add(region.get)
        return true
      }
    }
    false
  }

  def recordLinkCompletion(linkId: LinkIdentity): Boolean = {
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
      if (isRegionCompleted(region.get)) {
        runningRegions.remove(region.get)
        completedRegions.add(region.get)
        return true
      }
    }
    false
  }

  def getNextRegionsToSchedule(): Set[PipelinedRegion]
}
