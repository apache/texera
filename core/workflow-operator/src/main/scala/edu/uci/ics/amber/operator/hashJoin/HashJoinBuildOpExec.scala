package edu.uci.ics.amber.operator.hashJoin

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class HashJoinBuildOpExec[K](descString: String) extends OperatorExecutor {
  private val desc: HashJoinOpDesc[K] =
    objectMapper.readValue(descString, classOf[HashJoinOpDesc[K]])
  var buildTableHashMap: mutable.HashMap[K, ListBuffer[Tuple]] = _

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    val key = tuple.getField(desc.buildAttributeName).asInstanceOf[K]
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
