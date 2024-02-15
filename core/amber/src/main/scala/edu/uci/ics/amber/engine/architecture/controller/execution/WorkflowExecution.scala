package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.amber.engine.architecture.scheduling.{Region, RegionIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.{ChannelIdentity, PhysicalOpIdentity}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState._

import scala.collection.mutable

class WorkflowExecution {

  private val regionExecutions: mutable.LinkedHashMap[RegionIdentity, RegionExecution] =
    mutable.LinkedHashMap()

  def initRegionExecution(region: Region): RegionExecution = {
    assert(!regionExecutions.contains(region.id))
    regionExecutions.getOrElseUpdate(region.id, RegionExecution(region))
  }
  def getRegionExecution(regionId: RegionIdentity): RegionExecution = regionExecutions(regionId)

  def getLatestOperatorExecution(physicalOpId: PhysicalOpIdentity): OperatorExecution = {
    regionExecutions.values.toList
      .findLast(regionExecution => regionExecution.hasOperatorExecution(physicalOpId))
      .get
      .getOperatorExecution(physicalOpId)
  }

  def hasOperatorExecution(physicalOpId: PhysicalOpIdentity): Boolean = {
    regionExecutions.values.toList.exists(regionExecution =>
      regionExecution.hasOperatorExecution(physicalOpId)
    )
  }
  def isCompleted: Boolean = getState == WorkflowAggregatedState.COMPLETED

  def getState: WorkflowAggregatedState = {
    val regionStates = regionExecutions.values.map(_.getState)
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

  def getRunningRegionExecutions: Iterable[RegionExecution] = {
    regionExecutions.values.filter(regionExecution => !regionExecution.isCompleted)
  }

}
