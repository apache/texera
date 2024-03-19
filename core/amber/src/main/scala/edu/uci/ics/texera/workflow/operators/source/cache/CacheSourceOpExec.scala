package edu.uci.ics.texera.workflow.operators.source.cache

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.SourceOperatorExecutor
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.operators.sink.storage.SinkStorageReader

class CacheSourceOpExec(storage: SinkStorageReader)
    extends SourceOperatorExecutor
    with LazyLogging {

  override def produceTuple(): Iterator[TupleLike] = storage.getAll.iterator

}
