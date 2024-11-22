package edu.uci.ics.amber.operator.source.cache

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.core.storage.result.SinkStorageReader
import edu.uci.ics.amber.core.executor.SourceOperatorExecutor

class CacheSourceOpExec(storage: SinkStorageReader)
    extends SourceOperatorExecutor
    with LazyLogging {

  override def produceTuple(): Iterator[TupleLike] = storage.getAll.iterator

}
