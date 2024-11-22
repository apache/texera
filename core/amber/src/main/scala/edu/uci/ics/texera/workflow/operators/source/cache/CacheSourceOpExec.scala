package edu.uci.ics.texera.workflow.operators.source.cache

import com.typesafe.scalalogging.LazyLogging

class CacheSourceOpExec(storage: SinkStorageReader)
  extends SourceOperatorExecutor
    with LazyLogging {

  override def produceTuple(): Iterator[TupleLike] = storage.getAll.iterator

}
