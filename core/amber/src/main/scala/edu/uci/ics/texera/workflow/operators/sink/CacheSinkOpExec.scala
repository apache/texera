package edu.uci.ics.texera.workflow.operators.sink

import edu.uci.ics.amber.engine.common.{ITupleSinkOperatorExecutor, InputExhausted}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import com.typesafe.scalalogging.Logger

class CacheSinkOpExec(dest: mutable.MutableList[Tuple]) extends ITupleSinkOperatorExecutor {

  assert(null != dest)

  private val logger = Logger(this.getClass.getName)

  val results: mutable.MutableList[Tuple] = mutable.MutableList()

  //TODO: Empty list or null?
  override def getResultTuples(): List[ITuple] = {
    logger.info("Get result tuples: {}.", results.toString())
    results.toList
  }

  override def getOutputMode(): IncrementalOutputMode = IncrementalOutputMode.SET_SNAPSHOT

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[ITuple] = {
    logger.info("Processing tuple {}", tuple.toString)
    tuple match {
      case Left(t) =>
        results += t.asInstanceOf[Tuple]
      case Right(_) =>
        //TODO: How to guarantee atomicity?
        dest.clear()
        results.foreach(tuple => dest += tuple)
    }
    Iterator()
  }
}
