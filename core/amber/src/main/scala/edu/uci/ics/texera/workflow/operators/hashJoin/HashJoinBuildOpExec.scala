package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class HashJoinBuildOpExec[K](buildAttributeName: String) extends OperatorExecutor {

  var buildTableHashMap: mutable.HashMap[K, ListBuffer[Tuple]] = _

  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[TupleLike] = {
    tuple match {
      case Left(tuple) =>
        val key = tuple.getField(buildAttributeName).asInstanceOf[K]
        buildTableHashMap.getOrElseUpdate(key, new ListBuffer[Tuple]()) += tuple
        Iterator()
      case Right(_) =>
        buildTableHashMap.iterator.flatMap {
          case (k, v) => v.map(t => TupleLike(List(k) ++ t.getFields))
        }
    }
  }

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, mutable.ListBuffer[Tuple]]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }
}
