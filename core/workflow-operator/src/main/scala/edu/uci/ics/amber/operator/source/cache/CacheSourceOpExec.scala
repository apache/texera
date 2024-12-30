package edu.uci.ics.amber.operator.source.cache

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.storage.result.ResultStorage
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.core.virtualidentity.WorkflowIdentity

class CacheSourceOpExec(storageKey: String, workflowIdentity: WorkflowIdentity)
    extends SourceOperatorExecutor
    with LazyLogging {
  private val storage = ResultStorage.getOpResultStorage(workflowIdentity).get(storageKey)

  override def produceTuple(): Iterator[TupleLike] = storage.get()

}
