package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.RangeBasedShufflePartitioning
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType

case class RangeBasedShufflePartitioner(partitioning: RangeBasedShufflePartitioning)
    extends Partitioner {

  val keysPerReceiver =
    ((partitioning.rangeMax - partitioning.rangeMin) / partitioning.receivers.length).toLong + 1

  override def getBucketIndex(tuple: Tuple): Iterator[Int] = {
    // Do range partitioning only on the first attribute in `rangeColumnIndices`.
    val fieldType = tuple.getSchema
      .getAttributes(partitioning.rangeColumnIndices(0))
      .getType
    var fieldVal: Long = -1
    fieldType match {
      case AttributeType.LONG =>
        fieldVal = tuple.getField[Long](partitioning.rangeColumnIndices(0))
      case AttributeType.INTEGER =>
        fieldVal = tuple.getField[Int](partitioning.rangeColumnIndices(0))
      case AttributeType.DOUBLE =>
        fieldVal = tuple.getField[Double](partitioning.rangeColumnIndices(0)).toLong
      case _ =>
        throw new RuntimeException("unsupported attribute type: " + fieldType.toString())
    }

    if (fieldVal < partitioning.rangeMin) {
      return Iterator(0)
    }
    if (fieldVal > partitioning.rangeMax) {
      return Iterator(partitioning.receivers.length - 1)
    }
    Iterator(((fieldVal - partitioning.rangeMin) / keysPerReceiver).toInt)
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = partitioning.receivers

}
