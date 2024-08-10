package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class HashJoinBuildOpExec[K](buildAttributeName: String) extends OperatorExecutor {

  var buildTableHashMap: mutable.HashMap[K, ListBuffer[Tuple]] = _

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    val key = tuple.getField(buildAttributeName).asInstanceOf[K]
    buildTableHashMap.getOrElseUpdate(key, new ListBuffer[Tuple]()) += tuple
    Iterator()
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    buildTableHashMap.iterator.flatMap {
      case (k, v) => v.map(t => TupleLike(List(k) ++ t.getFields))
    }
  }

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, mutable.ListBuffer[Tuple]]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }
}