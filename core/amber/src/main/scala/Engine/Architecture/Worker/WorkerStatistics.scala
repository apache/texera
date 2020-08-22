package Engine.Architecture.Worker

case class WorkerStatistics(
    workerState: WorkerState.Value,
    inputRowCount: Long,
    outputRowCount: Long
)
