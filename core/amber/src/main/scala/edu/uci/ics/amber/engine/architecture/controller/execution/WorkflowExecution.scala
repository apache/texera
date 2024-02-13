package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.amber.engine.architecture.scheduling.{Region, RegionExecution, RegionIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity, PhysicalOpIdentity}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState._
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}

import scala.collection.mutable

class WorkflowExecution {

  private val regionExecutions: mutable.LinkedHashMap[RegionIdentity, RegionExecution] =
    mutable.LinkedHashMap()

  private val channelExecutions: mutable.Map[ChannelIdentity, ChannelExecution] = mutable.HashMap()

  def initRegionExecution(region: Region): RegionExecution = {
    assert(!regionExecutions.contains(region.id))
    regionExecutions.getOrElseUpdate(region.id, RegionExecution(region))
  }
  def getRegionExecution(regionId: RegionIdentity): RegionExecution = regionExecutions(regionId)

  def getOperatorExecution(physicalOpId: PhysicalOpIdentity): Option[OperatorExecution] = {
    regionExecutions.values.toList
      .findLast(regionExecution => regionExecution.hasOperatorExecution(physicalOpId)).map(_.getOperatorExecution(physicalOpId))
  }

  def getAllOperatorExecutions: Iterator[(PhysicalOpIdentity, OperatorExecution)] = {
    regionExecutions.values
      .flatMap(regionExecution => regionExecution.getAllOperatorExecutions)
      .toMap
      .iterator
  }

  def getAllBuiltWorkers: Iterator[ActorVirtualIdentity] = {
    regionExecutions.values
      .flatMap(regionExecution => regionExecution.getAllOperatorExecutions)
      .map(_._2)
      .flatMap(operatorExecution => operatorExecution.getWorkerIds).iterator
  }
  def initChannelExecution(channelId: ChannelIdentity): ChannelExecution = {
    assert(!channelExecutions.contains(channelId))
    channelExecutions.getOrElseUpdate(channelId, ChannelExecution())
  }

  def getChannelExecutions: Iterable[(ChannelIdentity, ChannelExecution)] = channelExecutions
  def getStats: Map[String, OperatorRuntimeStats] = {
    val activeRegionExecutions = regionExecutions.values.filterNot(_.isCompleted)

//
//    // TODO: fix the aggregation here. The stats should be on port level.
    getAllOperatorExecutions.map {
      case (physicalOpId, operatorExecution) =>
        physicalOpId.logicalOpId.id -> operatorExecution.getStats
    }.toMap
  }

  def isCompleted: Boolean = getState == WorkflowAggregatedState.COMPLETED

  def getState: WorkflowAggregatedState = {
    val activeRegionExecutions = regionExecutions.values
    val regionStates = activeRegionExecutions.map(_.getState)
    println(regionStates)
    if (regionStates.isEmpty) {
      return WorkflowAggregatedState.UNINITIALIZED
    }
    if (regionStates.forall(_ == COMPLETED)) {
      return WorkflowAggregatedState.COMPLETED
    }
    if (regionStates.exists(_ == RUNNING)) {
      return WorkflowAggregatedState.RUNNING
    }
    val unCompletedOpStates = regionStates.filter(_ != COMPLETED)
    val runningOpStates = unCompletedOpStates.filter(_ != UNINITIALIZED)
    if (unCompletedOpStates.forall(_ == UNINITIALIZED)) {
      return WorkflowAggregatedState.UNINITIALIZED
    }
    if (runningOpStates.forall(_ == PAUSED)) {
      WorkflowAggregatedState.PAUSED
    } else if (runningOpStates.forall(_ == READY)) {
      WorkflowAggregatedState.READY
    } else {
      WorkflowAggregatedState.UNKNOWN
    }
  }

}
