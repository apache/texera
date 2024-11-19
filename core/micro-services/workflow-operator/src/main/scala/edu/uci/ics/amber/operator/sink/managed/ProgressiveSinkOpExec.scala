package edu.uci.ics.amber.operator.sink.managed

import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.core.storage.result.SinkStorageWriter
import edu.uci.ics.amber.operator.sink.IncrementalOutputMode.{SET_DELTA, SET_SNAPSHOT}
import edu.uci.ics.amber.core.executor.SinkOperatorExecutor
import edu.uci.ics.amber.workflow.PortIdentity
import edu.uci.ics.amber.operator.sink.IncrementalOutputMode

class ProgressiveSinkOpExec(outputMode: IncrementalOutputMode, storage: SinkStorageWriter)
    extends SinkOperatorExecutor {

  override def open(): Unit = {
    storage.open()
  }

  override def consumeTuple(
      tuple: Tuple,
      input: Int
  ): Unit = {
    outputMode match {
      case SET_SNAPSHOT => updateSetSnapshot(tuple)
      case SET_DELTA    => storage.putOne(tuple)
    }
  }

  private def updateSetSnapshot(deltaUpdate: Tuple): Unit = {
    val (isInsertion, tupleValue) = ProgressiveUtils.getTupleFlagAndValue(deltaUpdate)

    if (isInsertion) {
      storage.putOne(tupleValue)
    } else {
      storage.removeOne(tupleValue)
    }
  }

  override def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] = {
    storage.close()
    Iterator.empty
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
}
