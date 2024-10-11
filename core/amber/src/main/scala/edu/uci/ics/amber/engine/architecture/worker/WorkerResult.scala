package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.common.IncrementalOutputMode
import edu.uci.ics.amber.engine.common.model.tuple.Tuple

case class WorkerResult(
    outputMode: IncrementalOutputMode,
    result: List[Tuple]
)
