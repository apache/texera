package edu.uci.ics.texera.workflow.operators.source.cache

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.executor.SourceOperatorExecutor
import edu.uci.ics.amber.engine.common.model.tuple.TupleLike
import edu.uci.ics.texera.workflow.operators.sink.storage.SinkStorageReader

class CacheSourceOpExec(storage: SinkStorageReader)
    extends SourceOperatorExecutor
    with LazyLogging {

  override def produceTuple(): Iterator[TupleLike] = storage.getAll.iterator

}
