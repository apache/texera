package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.common.statetransition2.WorkerState
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode

case class WorkerStatistics(
    workerState: WorkerState,
    inputRowCount: Long,
    outputRowCount: Long
)

case class WorkerResult(
    outputMode: IncrementalOutputMode,
    result: List[ITuple]
)
