package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.amber.engine.architecture.controller.execution.ExecutionUtils.aggregateMetrics
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState._
import edu.uci.ics.amber.engine.architecture.scheduling.{Region, RegionIdentity}
import edu.uci.ics.amber.engine.common.executionruntimestate.OperatorMetrics
import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity

import scala.collection.mutable

case class WorkflowExecution() {

  // region executions are stored with LinkedHashMap to maintain their creation order.
  private val regionExecutions: mutable.LinkedHashMap[RegionIdentity, RegionExecution] =
    mutable.LinkedHashMap()

  /**
    * Initializes or retrieves a `RegionExecution` for a given `Region`. If not already
    * initialized, it creates and returns a new `RegionExecution`; otherwise, an assertion
    * error is thrown if re-initialization is attempted.
    *
    * @param region The `Region` for which to initialize or retrieve the `RegionExecution`.
    * @return The `RegionExecution` associated with the given `Region`.
    * @throws AssertionError if the `RegionExecution` has already been initialized.
    */
  def initRegionExecution(region: Region): RegionExecution = {
    // ensure the region execution hasn't been initialized already.
    assert(
      !regionExecutions.contains(region.id),
      s"RegionExecution of ${region.id} already initialized."
    )
    regionExecutions.getOrElseUpdate(region.id, RegionExecution(region))
  }

  /**
    * Retrieves a specific `RegionExecution` by its identifier.
    *
    * @param regionId The unique identifier of the region for which the execution is to be retrieved.
    * @return The `RegionExecution` associated with the specified `regionId`.
    */
  def getRegionExecution(regionId: RegionIdentity): RegionExecution = regionExecutions(regionId)

  /**
    * Retrieves all `RegionExecutions` that are currently in running state,
    * preserving the order in which they were created.
    *
    * This method filters the executions to include only those that have not completed.
    *
    * @return An `Iterable` of `RegionExecution` objects that are in running state.
    */
  def getRunningRegionExecutions: Iterable[RegionExecution] = {
    regionExecutions.values.filterNot(_.isCompleted)
  }

  /**
    * Retrieve the runtime stats of all `RegionExecutions`
    *
    * @return A `Map` with key being `Logical Operator ID` and the value being operator runtime statistics
    */
  def getAllRegionExecutionsStats: Map[String, OperatorMetrics] = {
    val allRegionExecutions: Iterable[RegionExecution] = getAllRegionExecutions

    val statsMap: Map[PhysicalOpIdentity, OperatorMetrics] = allRegionExecutions.flatMap {
      regionExecution =>
        regionExecution.getStats.map {
          case (physicalOpIdentity, operatorMetrics) =>
            (physicalOpIdentity, operatorMetrics)
        }
    }.toMap

    val aggregatedStats: Map[String, OperatorMetrics] =
      statsMap.groupBy(_._1.logicalOpId.id).map {
        case (logicalOpId, stats) =>
          (logicalOpId, aggregateMetrics(stats.values))
      }
    aggregatedStats
  }

  /**
    * Retrieves all `RegionExecutions`, preserving the order in which they were created.
    *
    * This method provides access to all executions, regardless of their state.
    *
    * @return An `Iterable` of all `RegionExecution` objects in the order they were added.
    */
  def getAllRegionExecutions: Iterable[RegionExecution] = regionExecutions.values

  /**
    * Retrieves the latest `OperatorExecution` associated with the specified physical operatorId.
    *
    * This method searches through all `RegionExecutions` in reverse creation order to find the most recent
    * `OperatorExecution` that matches the given physical operatorId. It assumes that each `RegionExecution`
    * may contain zero or exactly one `OperatorExecution` instance, and it returns the latest one found that
    * corresponds to the specified operatorId.
    *
    * @param physicalOpId The unique identifier of the physical operator for which the latest execution is
    *                     to be retrieved.
    * @return The latest `OperatorExecution` instance associated with the given physical operatorId.
    * @throws NoSuchElementException if no `OperatorExecution` is found for the specified operatorId.
    */
  def getLatestOperatorExecution(physicalOpId: PhysicalOpIdentity): OperatorExecution = {
    regionExecutions.values.toList
      .findLast(regionExecution => regionExecution.hasOperatorExecution(physicalOpId))
      .get
      .getOperatorExecution(physicalOpId)
  }

  def isCompleted: Boolean = getState == WorkflowAggregatedState.COMPLETED

  def getState: WorkflowAggregatedState = {
    val regionStates = regionExecutions.values.map(_.getState)
    if (regionStates.isEmpty) {
      return WorkflowAggregatedState.UNINITIALIZED
    }
    if (regionStates.forall(_ == COMPLETED)) {
      return WorkflowAggregatedState.COMPLETED
    }
    val unCompletedOpStates = regionExecutions.values
      .filter(_.getState != COMPLETED)
      .flatMap(_.getAllOperatorExecutions.map(_._2.getState))
      .filter(_ != COMPLETED)
    if (unCompletedOpStates.forall(_ == UNINITIALIZED)) {
      return WorkflowAggregatedState.UNINITIALIZED
    }
    val runningOpStates = unCompletedOpStates.filter(_ != UNINITIALIZED)
    if (runningOpStates.exists(_ == RUNNING)) {
      WorkflowAggregatedState.RUNNING
    } else if (runningOpStates.forall(_ == PAUSED)) {
      WorkflowAggregatedState.PAUSED
    } else if (runningOpStates.forall(_ == READY)) {
      WorkflowAggregatedState.READY
    } else {
      WorkflowAggregatedState.UNKNOWN
    }
  }

}
