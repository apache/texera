package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState

object ExecutionUtils {
  def aggregateStates[T](
      states: List[T],
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
