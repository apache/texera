package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.amber.engine.architecture.worker.statistics.PortTupleCountMapping
import edu.uci.ics.amber.engine.common.workflowruntimestate.{
  OperatorMetrics,
  OperatorStatistics,
  WorkflowAggregatedState
}

object ExecutionUtils {

  /**
    * Handle the case when a logical operator has two physical operators within a same region (e.g., Aggregate operator)
    */
  def aggregateMetrics(metrics: Iterable[OperatorMetrics]): OperatorMetrics = {
    val aggregatedState = aggregateStates(
      metrics.map(_.operatorState),
      WorkflowAggregatedState.COMPLETED,
      WorkflowAggregatedState.RUNNING,
      WorkflowAggregatedState.UNINITIALIZED,
      WorkflowAggregatedState.PAUSED,
      WorkflowAggregatedState.READY
    )

    val inputCountSum = metrics
      .flatMap(_.operatorStatistics.inputCount)
      .filter(!_.portId.internal)
      .groupBy(_.portId)
      .map {
        case (k, v) =>
          k -> v.map(_.tupleCount).sum
      }
      .map { case (portId, tuple_count) => new PortTupleCountMapping(portId, tuple_count) }
      .toSeq
    val outputCountSum = metrics
      .flatMap(_.operatorStatistics.outputCount)
      .filter(!_.portId.internal)
      .groupBy(_.portId)
      .map {
        case (k, v) =>
          k -> v.map(_.tupleCount).sum
      }
      .map { case (portId, tuple_count) => new PortTupleCountMapping(portId, tuple_count) }
      .toSeq
    val numWorkersSum = metrics.map(_.operatorStatistics.numWorkers).sum
    val dataProcessingTimeSum = metrics.map(_.operatorStatistics.dataProcessingTime).sum
    val controlProcessingTimeSum = metrics.map(_.operatorStatistics.controlProcessingTime).sum
    val idleTimeSum = metrics.map(_.operatorStatistics.idleTime).sum

    OperatorMetrics(
      aggregatedState,
      OperatorStatistics(
        inputCountSum,
        outputCountSum,
        numWorkersSum,
        dataProcessingTimeSum,
        controlProcessingTimeSum,
        idleTimeSum
      )
    )
  }
  def aggregateStates[T](
      states: Iterable[T],
      completedState: T,
      runningState: T,
      uninitializedState: T,
      pausedState: T,
      readyState: T
  ): WorkflowAggregatedState = {
    if (states.isEmpty) {
      WorkflowAggregatedState.UNINITIALIZED
    } else if (states.forall(_ == completedState)) {
      WorkflowAggregatedState.COMPLETED
    } else if (states.exists(_ == runningState)) {
      WorkflowAggregatedState.RUNNING
    } else {
      val unCompletedStates = states.filter(_ != completedState)
      if (unCompletedStates.forall(_ == uninitializedState)) {
        WorkflowAggregatedState.UNINITIALIZED
      } else if (unCompletedStates.forall(_ == pausedState)) {
        WorkflowAggregatedState.PAUSED
      } else if (unCompletedStates.forall(_ == readyState)) {
        WorkflowAggregatedState.RUNNING
      } else {
        WorkflowAggregatedState.UNKNOWN
      }
    }
  }
}
