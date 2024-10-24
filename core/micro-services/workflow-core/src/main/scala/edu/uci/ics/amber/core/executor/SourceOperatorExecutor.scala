package edu.uci.ics.amber.core.executor

import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.storage.dataset.DatasetFileDocument
import edu.uci.ics.amber.workflow.PortIdentity

import java.io.{FileInputStream, InputStream}

trait SourceOperatorExecutor extends OperatorExecutor {
  override def open(): Unit = {}

  override def close(): Unit = {}
  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = Iterator()

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty

  def produceTuple(): Iterator[TupleLike]

  override def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] = {
    // We assume there is only one input port for source operators. The current assumption
    // makes produceTuple to be invoked on each input port finish.
    // We should move this to onFinishAllPorts later.
    produceTuple().map(t => (t, Option.empty))
  }

  // this function create the input stream accordingly:
  // - if filePath is set, create the stream from the file
  // - if fileDesc is set, create the stream via JGit call
  def createInputStream(filePath: String, datasetFileDocument: DatasetFileDocument): InputStream = {
    if (filePath != null && datasetFileDocument != null) {
      throw new RuntimeException(
        "File Path and Dataset File Descriptor cannot present at the same time."
      )
    }
    if (filePath != null) {
      new FileInputStream(filePath)
    } else {
      // create stream from dataset file desc
      datasetFileDocument.asInputStream()
    }
  }
}
