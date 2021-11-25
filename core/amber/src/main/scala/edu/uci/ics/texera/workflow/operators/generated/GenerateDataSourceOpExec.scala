package edu.uci.ics.texera.workflow.operators.generated

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.collection.mutable.ArrayBuffer

class GenerateDataSourceOpExec(limit:Int,
                               schema:Schema,
                               keyRange: (Int,Int),
                               distributions: ArrayBuffer[(Int, Array[(Int,Int)])],
                               delay: (Int, Int)) extends SourceOperatorExecutor {

  var current = 0
  var keyCur = keyRange._1
  var keyCurCount = 0
  var keyDist = Array.range(keyRange._1, keyRange._2+1).map{
    key => key -> 100
  }.toMap
  override def produceTexeraTuple(): Iterator[Tuple] = {
    new Iterator[Tuple]{
      override def hasNext: Boolean = current < limit

      override def next(): Tuple = {
        if(current > 0 && delay._1 > 0 && (current % delay._1) == 0 && delay._2 > 0){
          Thread.sleep(delay._2)
        }
        if(distributions.nonEmpty && current == distributions.head._1){
          val curDist = distributions.remove(0)
          val total = 100*(keyRange._2 - keyRange._1 + 1)
          var percentage = 100
          val skewedMap = curDist._2.map{
            case (key,v) =>
              percentage -= v
              key -> (total/100)*v
          }.toMap
          val remaining = total/100*percentage
          val remainingKeyCount = (keyRange._2 - keyRange._1 + 1) - curDist._2.length
          keyDist = skewedMap ++ Array.range(keyRange._1, keyRange._2+1).map{
            case key if !skewedMap.contains(key) => key -> remaining/remainingKeyCount
          }.toMap
          println("new key dist = "+keyDist)
        }
        if(keyDist(keyCur) <= keyCurCount){
          keyCur += 1
          if(keyCur == keyRange._2){
            keyCur = keyRange._1
          }
          keyCurCount = 0
        }
        val res = Tuple.newBuilder().add(schema,Array[AnyRef](Int.box(keyCur), Int.box(current))).build()
        keyCurCount +=1
        current += 1
        res
      }
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
