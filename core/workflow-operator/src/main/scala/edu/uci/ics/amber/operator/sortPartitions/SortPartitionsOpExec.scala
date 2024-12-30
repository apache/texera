package edu.uci.ics.amber.operator.sortPartitions

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{AttributeType, Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import scala.collection.mutable.ArrayBuffer

class SortPartitionsOpExec(
    descString: String
) extends OperatorExecutor {
  private val desc: SortPartitionsOpDesc =
    objectMapper.readValue(descString, classOf[SortPartitionsOpDesc])
  private var unorderedTuples: ArrayBuffer[Tuple] = _

  private def sortTuples(): Iterator[TupleLike] = unorderedTuples.sortWith(compareTuples).iterator

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    unorderedTuples.append(tuple)
    Iterator()
  }

  override def onFinish(port: Int): Iterator[TupleLike] = sortTuples()

  private def compareTuples(t1: Tuple, t2: Tuple): Boolean = {
    val attributeType = t1.getSchema.getAttribute(desc.sortAttributeName).getType
    val attributeIndex = t1.getSchema.getIndex(desc.sortAttributeName)
    attributeType match {
      case AttributeType.LONG =>
        t1.getField[Long](attributeIndex) < t2.getField[Long](attributeIndex)
      case AttributeType.INTEGER =>
        t1.getField[Int](attributeIndex) < t2.getField[Int](attributeIndex)
      case AttributeType.DOUBLE =>
        t1.getField[Double](attributeIndex) < t2.getField[Double](attributeIndex)
      case _ =>
        true // unsupported type
    }
  }

  override def open(): Unit = {
    unorderedTuples = new ArrayBuffer[Tuple]()
  }

  override def close(): Unit = {
    unorderedTuples.clear()
  }

}
