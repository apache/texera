package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.State
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class HashJoinBuildOpExec[K](buildAttributeName: String) extends OperatorExecutor {

  var buildTableHashMap: mutable.HashMap[K, (ListBuffer[Tuple], Boolean)] = _

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    val key = tuple.getField(buildAttributeName).asInstanceOf[K]
    buildTableHashMap.getOrElseUpdate(key, (new ListBuffer[Tuple](), false))._1 += tuple
    Iterator()
  }


  override def onFinishProduceState(prot: Int): State = {
    val state = State()
    state.add("hashtable", AttributeType.ANY, buildTableHashMap)
    state
  }

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, (mutable.ListBuffer[Tuple], Boolean)]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }
}