package edu.uci.ics.texera.workflow.operators.source.cache

import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class CacheSourceOpExec(uuid: String, opResultStorage: OpResultStorage)
    extends SourceOperatorExecutor {
  assert(null != uuid)
  assert(null != opResultStorage)

  private val logger = Logger(this.getClass.getName)

  override def produceTexeraTuple(): Iterator[Tuple] = {
    assert(null != uuid)
    logger.info("Retrieve cached output from {}.", this.toString)
    opResultStorage.get(uuid).iterator
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
