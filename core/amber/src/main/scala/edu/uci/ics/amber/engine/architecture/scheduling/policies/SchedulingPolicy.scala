package edu.uci.ics.amber.engine.architecture.scheduling.policies

import akka.actor.ActorContext
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowStatusUpdate
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RegionsTimeSlotExpiredHandler.RegionsTimeSlotExpired
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
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
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

object SchedulingPolicy {
  def createPolicy(
      policyName: String,
      workflow: Workflow,
      ctx: ActorContext,
      asyncRPCClient: AsyncRPCClient
  ): SchedulingPolicy = {
    if (policyName.equals("single-ready-region")) {
      new SingleReadyRegion(workflow, ctx, asyncRPCClient)
    } else if (policyName.equals("all-ready-regions")) {
      new AllReadyRegions(workflow, ctx, asyncRPCClient)
    } else if (policyName.equals("all-ready-time-interleaved-regions")) {
      new AllReadyTimeInterleavedRegions(workflow, ctx, asyncRPCClient)
    } else {
      throw new WorkflowRuntimeException(s"Unknown scheduling policy name")
    }
  }
}

abstract class SchedulingPolicy(
    workflow: Workflow,
    ctx: ActorContext,
    asyncRPCClient: AsyncRPCClient
) {

  protected val regionsScheduleOrder = saveTopologicalOrderOfRegions()

  protected val sentToBeScheduledRegions =
    new mutable.HashSet[
      PipelinedRegion
    ]() // regions sent by the policy to be scheduled at least once
  protected val completedRegions = new mutable.HashSet[PipelinedRegion]()
  protected val runningRegions = new mutable.HashSet[PipelinedRegion]() // regions currently running
  protected val completedLinksOfRegion =
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

  protected def isRegionCompleted(region: PipelinedRegion): Boolean = {
    workflow
      .getBlockingOutlinksOfRegion(region)
      .forall(
        completedLinksOfRegion.getOrElse(region, new mutable.HashSet[LinkIdentity]()).contains
      ) && region
      .getOperators()
      .forall(opId => workflow.getOperator(opId).getState == WorkflowAggregatedState.COMPLETED)
  }

  protected def checkRegionCompleted(region: PipelinedRegion): Unit = {
    if (isRegionCompleted(region)) {
      runningRegions.remove(region)
      completedRegions.add(region)
    }
  }

  protected def getRegion(workerId: ActorVirtualIdentity): Option[PipelinedRegion] = {
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
  protected def getRegion(linkId: LinkIdentity): Option[PipelinedRegion] = {
    val upstreamOpId = OperatorIdentity(linkId.from.workflow, linkId.from.operator)
    var region: Option[PipelinedRegion] = None
    runningRegions.foreach(r =>
      if (r.getOperators().contains(upstreamOpId)) {
        region = Some(r)
      }
    )
    region
  }

  // gets the ready regions that is not currently running
  protected def getNextSchedulingWork(): Set[PipelinedRegion]

  def recordWorkerCompletion(workerId: ActorVirtualIdentity): Set[PipelinedRegion] = {
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

  def recordLinkCompletion(linkId: LinkIdentity): Set[PipelinedRegion] = {
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

  def startWorkflow(): Set[PipelinedRegion] = {
    val regions = getNextSchedulingWork()
    if (regions.isEmpty) {
      throw new WorkflowRuntimeException(
        s"No first region is being scheduled"
      )
    }
    regions
  }

  def recordTimeSlotExpired(): Set[PipelinedRegion] = {
    getNextSchedulingWork()
  }

  def addToRunningRegions(regions: Set[PipelinedRegion]): Unit = {
    regions.foreach(r => runningRegions.add(r))
  }

  def removeFromRunningRegion(regions: Set[PipelinedRegion]): Unit = {
    regions.foreach(r => runningRegions.remove(r))
  }

  def getRunningRegions(): Set[PipelinedRegion] = runningRegions.toSet

  def getCompletedRegions(): Set[PipelinedRegion] = completedRegions.toSet

}
