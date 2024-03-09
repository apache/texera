package edu.uci.ics.texera.workflow.operators.sink.managed

import edu.uci.ics.amber.engine.common.{ISinkOperatorExecutor, InputExhausted}
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode._
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.{IncrementalOutputMode, ProgressiveUtils}
import edu.uci.ics.texera.workflow.operators.sink.storage.SinkStorageWriter

class ProgressiveSinkOpExec(
    outputMode: IncrementalOutputMode,
    storage: SinkStorageWriter
) extends ISinkOperatorExecutor {
  override def consume(
      tuple: Tuple,
      input: Int
  ): Unit = {
    outputMode match {
      case SET_SNAPSHOT =>
        updateSetSnapshot(tuple)
      case SET_DELTA =>
        storage.putOne(tuple)
    }
  }

  private def updateSetSnapshot(deltaUpdate: Tuple): Unit = {
    val (isInsertion, tupleValue) =
      ProgressiveUtils.getTupleFlagAndValue(deltaUpdate)
    if (isInsertion) {
      storage.putOne(tupleValue)
    } else {
      storage.removeOne(tupleValue)
    }
  }

}
