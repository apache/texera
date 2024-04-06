package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}

object ExecutionUtils {

  /**
    * Handle the case when a logical operator has two physical operators within a same region (e.g., Aggregate operator)
    */
  def aggregateStats(stats: Iterable[OperatorRuntimeStats]): OperatorRuntimeStats = {
    val aggregatedState = aggregateStates(
      stats.map(_.state.value),
      WorkflowAggregatedState.COMPLETED.value,
      WorkflowAggregatedState.RUNNING.value,
      WorkflowAggregatedState.UNINITIALIZED.value,
      WorkflowAggregatedState.PAUSED.value,
      WorkflowAggregatedState.READY.value
    )

    val inputCountSum = stats.flatMap(_.inputCount).groupBy(_._1).map {
      case (k, v) =>
        k -> v.map(_._2).sum
    }
    val outputCountSum = stats.flatMap(_.outputCount).groupBy(_._1).map {
      case (k, v) =>
        k -> v.map(_._2).sum
    }
    val numWorkersSum = stats.map(_.numWorkers).sum
    val dataProcessingTimeSum = stats.map(_.dataProcessingTime).sum
    val controlProcessingTimeSum = stats.map(_.controlProcessingTime).sum
    val idleTimeSum = stats.map(_.idleTime).sum

    OperatorRuntimeStats(
      aggregatedState,
      inputCountSum,
      outputCountSum,
      numWorkersSum,
      dataProcessingTimeSum,
      controlProcessingTimeSum,
      idleTimeSum
    )
  }
  def aggregateStates(
      states: Iterable[Int],
      completedState: Int,
      runningState: Int,
      uninitializedState: Int,
      pausedState: Int,
      readyState: Int
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
