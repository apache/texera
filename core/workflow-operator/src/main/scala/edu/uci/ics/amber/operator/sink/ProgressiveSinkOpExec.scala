package edu.uci.ics.amber.operator.sink

import edu.uci.ics.amber.core.executor.SinkOperatorExecutor
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import edu.uci.ics.amber.core.storage.result.ExecutionResourcesMapping
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.PortIdentity

import java.net.URI

class ProgressiveSinkOpExec(
    workerId: Int,
    outputMode: OutputMode,
    storageURI: URI
) extends SinkOperatorExecutor {
  val (doc, _) = DocumentFactory.openDocument(storageURI)
  val writer: BufferedItemWriter[Tuple] =
    doc.writer(workerId.toString).asInstanceOf[BufferedItemWriter[Tuple]]

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
