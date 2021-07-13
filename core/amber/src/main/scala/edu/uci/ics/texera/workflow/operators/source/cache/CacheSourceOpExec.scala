package edu.uci.ics.texera.workflow.operators.source.cache

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import com.typesafe.scalalogging.Logger

class CacheSourceOpExec(src: mutable.MutableList[Tuple]) extends SourceOperatorExecutor {
  assert(null != src)

  private val logger = Logger(this.getClass.getName)

  override def produceTexeraTuple(): Iterator[Tuple] = {
    logger.info("Retrieve cached output from {}.", this.toString)
    logger.info("Cached tuples: {}.", src.toString())
    src.iterator
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
