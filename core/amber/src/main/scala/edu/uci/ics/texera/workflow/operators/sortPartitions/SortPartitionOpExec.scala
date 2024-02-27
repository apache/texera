package edu.uci.ics.texera.workflow.operators.sortPartitions

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType

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

  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[TupleLike] = {
    tuple match {
      case Left(t) =>
        unorderedTuples.append(t)
        Iterator()
      case Right(_) =>
        sortTuples()
    }
  }

  private def compareTuples(t1: Tuple, t2: Tuple): Boolean = {
    val attributeType = t1.getSchema.getAttribute(sortAttributeName).getType
    val attributeIndex = t1.getSchema.getIndex(sortAttributeName)
    attributeType match {
      case AttributeType.LONG =>
        t1.get(attributeIndex).asInstanceOf[Long] < t2.get(attributeIndex).asInstanceOf[Long]
      case AttributeType.INTEGER =>
        t1.get(attributeIndex).asInstanceOf[Int] < t2.get(attributeIndex).asInstanceOf[Int]
      case AttributeType.DOUBLE =>
        t1.get(attributeIndex).asInstanceOf[Double] < t2.get(attributeIndex).asInstanceOf[Double]
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
