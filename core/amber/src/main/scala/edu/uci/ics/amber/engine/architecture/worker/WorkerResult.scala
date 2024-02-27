package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.workflow.common.tuple.Tuple

case class WorkerResult(
    outputMode: IncrementalOutputMode,
    result: List[Tuple]
)
