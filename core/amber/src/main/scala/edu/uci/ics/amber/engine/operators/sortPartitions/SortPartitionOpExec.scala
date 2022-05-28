package edu.uci.ics.amber.engine.operators.sortPartitions

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo}
import scala.collection.mutable.ArrayBuffer

class SortPartitionOpExec(
    sortAttributeName: String,
    operatorSchemaInfo: OperatorSchemaInfo
) extends OperatorExecutor {

  var tuplesToSort: ArrayBuffer[Tuple] = _

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        tuplesToSort.append(t)
        Iterator()
      case Right(_) =>
        tuplesToSort.sortWith(compareTuples).iterator
    }
  }

  def compareTuples(t1: Tuple, t2: Tuple): Boolean = {
    val attributeType = t1.getSchema().getAttribute(sortAttributeName).getType()
    val attributeIndex = t1.getSchema().getIndex(sortAttributeName)
    attributeType match {
      case AttributeType.LONG =>
        t1.getLong(attributeIndex) < t2.getLong(attributeIndex)
      case AttributeType.INTEGER =>
        t1.getInt(attributeIndex) < t2.getInt(attributeIndex)
      case AttributeType.DOUBLE =>
        t1.getDouble(attributeIndex) < t2.getDouble(attributeIndex)
      case _ =>
        true // unsupported type
    }
  }

  override def open = {
    tuplesToSort = new ArrayBuffer[Tuple]()
  }

  override def close = {
    tuplesToSort.clear()
  }

}
