package edu.uci.ics.texera.workflow.operators.sortPartitions

import edu.uci.ics.amber.engine.common.executor.OperatorExecutor
import edu.uci.ics.amber.engine.common.model.tuple.{AttributeType, Tuple, TupleLike}

import scala.collection.mutable.ArrayBuffer

class SortPartitionOpExec(
    sortAttributeName: String,
    localIdx: Int,
    domainMin: Long,
    domainMax: Long,
    numberOfWorkers: Int
) extends OperatorExecutor {

  private var unorderedTuples: ArrayBuffer[Tuple] = _

  private def sortTuples(): Iterator[TupleLike] = unorderedTuples.sortWith(compareTuples).iterator

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    unorderedTuples.append(tuple)
    Iterator()
  }

  override def onFinish(port: Int): Iterator[TupleLike] = sortTuples()

  private def compareTuples(t1: Tuple, t2: Tuple): Boolean = {
    val attributeType = t1.getSchema.getAttribute(sortAttributeName).getType
    val attributeIndex = t1.getSchema.getIndex(sortAttributeName)
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
