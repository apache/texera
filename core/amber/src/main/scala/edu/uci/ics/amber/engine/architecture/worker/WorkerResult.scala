package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.common.model.tuple.Tuple
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode

case class WorkerResult(
    outputMode: IncrementalOutputMode,
    result: List[Tuple]
)
