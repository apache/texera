package edu.uci.ics.texera.workflow.operators.source.cache

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.sink.storage.SinkStorageReader

class CacheSourceOpExec(storage: SinkStorageReader)
    extends SourceOperatorExecutor
    with LazyLogging {

  override def produceTexeraTuple(): Iterator[Tuple] = {
    storage.getAll.iterator
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
