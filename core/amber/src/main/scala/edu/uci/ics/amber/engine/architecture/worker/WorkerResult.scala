package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.operator.sink.IncrementalOutputMode

case class WorkerResult(
    outputMode: IncrementalOutputMode,
    result: List[Tuple]
)
