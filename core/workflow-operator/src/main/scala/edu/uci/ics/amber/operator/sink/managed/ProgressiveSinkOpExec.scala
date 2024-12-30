package edu.uci.ics.amber.operator.sink.managed

import edu.uci.ics.amber.core.executor.SinkOperatorExecutor
import edu.uci.ics.amber.core.storage.model.BufferedItemWriter
import edu.uci.ics.amber.core.storage.result.ResultStorage
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.operator.sink.ProgressiveUtils
import edu.uci.ics.amber.core.virtualidentity.WorkflowIdentity
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.PortIdentity

class ProgressiveSinkOpExec(
    outputMode: OutputMode,
    storageKey: String,
    workflowIdentity: WorkflowIdentity
) extends SinkOperatorExecutor {
  val writer: BufferedItemWriter[Tuple] =
    ResultStorage.getOpResultStorage(workflowIdentity).get(storageKey).writer()

  override def open(): Unit = {
    writer.open()
  }

  override def consumeTuple(
      tuple: Tuple,
      input: Int
  ): Unit = {
    outputMode match {
      case OutputMode.SET_SNAPSHOT | OutputMode.SINGLE_SNAPSHOT => updateSetSnapshot(tuple)
      case OutputMode.SET_DELTA                                 => writer.putOne(tuple)
      case _                                                    => throw new UnsupportedOperationException("Unsupported output mode")
    }
  }

  private def updateSetSnapshot(deltaUpdate: Tuple): Unit = {
    val (isInsertion, tupleValue) = ProgressiveUtils.getTupleFlagAndValue(deltaUpdate)

    if (isInsertion) {
      writer.putOne(tupleValue)
    } else {
      writer.removeOne(tupleValue)
    }
  }

  override def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] = {
    Iterator.empty
  }

  override def close(): Unit = {
    writer.close()
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
}
