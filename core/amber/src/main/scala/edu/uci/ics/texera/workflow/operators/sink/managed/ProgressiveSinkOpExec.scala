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

  override def open(): Unit = storage.open()

  override def close(): Unit = storage.close()

  override def consume(
      tuple: Either[Tuple, InputExhausted],
      input: Int
  ): Unit = {
    tuple match {
      case Left(t) =>
        outputMode match {
          case SET_SNAPSHOT =>
            updateSetSnapshot(t)
          case SET_DELTA =>
            storage.putOne(t)
        }
      case Right(_) => // skip
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
