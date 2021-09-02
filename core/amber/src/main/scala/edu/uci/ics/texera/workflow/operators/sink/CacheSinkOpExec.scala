package edu.uci.ics.texera.workflow.operators.sink

import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.storage.OpResultStorage
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.engine.common.{ITupleSinkOperatorExecutor, InputExhausted}
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable

class CacheSinkOpExec(uuid: String, dest: OpResultStorage) extends ITupleSinkOperatorExecutor {

  assert(null != dest)

  val results: mutable.MutableList[Tuple] = mutable.MutableList()
  private val logger = Logger(this.getClass.getName)

  override def getResultTuples(): List[ITuple] = {
    logger.info("Get resultt tuples.")
    val tuples = dest.get(uuid)
    assert(null != tuples)
    logger.info("result tuples length: {}", tuples.length)
    tuples
  }

  override def getOutputMode(): IncrementalOutputMode = IncrementalOutputMode.SET_SNAPSHOT

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[ITuple] = {
    logger.debug("Processing tuple {}", tuple.toString)
    tuple match {
      case Left(t) => results += t.asInstanceOf[Tuple]
      case Right(_) =>
        dest.remove(uuid)
        dest.put(uuid, results.toList)
    }
    Iterator()
  }
}
