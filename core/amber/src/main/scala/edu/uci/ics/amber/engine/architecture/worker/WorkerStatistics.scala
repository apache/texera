package edu.uci.ics.amber.engine.architecture.worker

case class WorkerStatistics(
    workerState: WorkerState.Value,
    inputRowCount: Long,
    outputRowCount: Long
)
