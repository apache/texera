package edu.uci.ics.texera.workflow.operators.source.cache

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class CacheSourceOpExec(uuid: String, opResultStorage: OpResultStorage)
    extends SourceOperatorExecutor
    with LazyLogging {
  assert(null != uuid)
  assert(null != opResultStorage)

  override def produceTexeraTuple(): Iterator[Tuple] = {
    assert(null != uuid)
    logger.debug("Retrieve cached output from {}.", this.toString)
    opResultStorage.get(uuid).iterator
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
