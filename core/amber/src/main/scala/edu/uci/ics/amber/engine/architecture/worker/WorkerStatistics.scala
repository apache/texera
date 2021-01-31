package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.WorkerState

case class WorkerStatistics(
    workerState: WorkerState,
    inputRowCount: Long,
    outputRowCount: Long
)
