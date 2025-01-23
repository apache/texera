package edu.uci.ics.amber.operator.hashJoin

import edu.uci.ics.amber.engine.common.{AmberRuntime, MerkleTreeFromByteArray}
import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class HashJoinBuildOpExec[K](descString: String) extends OperatorExecutor {
  private val desc: HashJoinOpDesc[K] =
    objectMapper.readValue(descString, classOf[HashJoinOpDesc[K]])
  var buildTableHashMap: mutable.HashMap[K, ListBuffer[Tuple]] = _
  var oldBytes:Array[Byte] = _
  var count = 0

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    val key = tuple.getField(desc.buildAttributeName).asInstanceOf[K]
    buildTableHashMap.getOrElseUpdate(key, new ListBuffer[Tuple]()) += tuple
    count +=1
    if(count > 100000 && count < 100100){
      val bytes = AmberRuntime.serde.serialize(buildTableHashMap).get
      println(s"Join state: ${bytes.length}")
      if (oldBytes != null) {
        val Abytes = MerkleTreeFromByteArray.getDiffInBytes(oldBytes, bytes, 4096)
        println(s"MarkleTree Diff: ${Abytes}")
      } else {
        val Abytes = MerkleTreeFromByteArray.getDiffInBytes(Array[Byte](), bytes, 4096)
        println(s"MarkleTree Diff: ${Abytes}")
      }
      oldBytes = bytes
      val bytes2 = AmberRuntime.serde.serialize(key.asInstanceOf[AnyRef]).get
      val bytes3 = AmberRuntime.serde.serialize(tuple).get
      println(s"Normal Diff: ${bytes2.length+bytes3.length}")
    }
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
