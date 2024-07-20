package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.RangeBasedShufflePartitioning
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType

case class RangeBasedShufflePartitioner(partitioning: RangeBasedShufflePartitioning)
    extends Partitioner {

  private val receivers = partitioning.channels.map(_.toWorkerId).distinct
  private val keysPerReceiver =
    ((partitioning.rangeMax - partitioning.rangeMin) / receivers.length) + 1

  override def getBucketIndex(tuple: Tuple): Iterator[Int] = {
    // Do range partitioning only on the first attribute in `rangeAttributeNames`.
    val attribute = tuple.getSchema.getAttribute(partitioning.rangeAttributeNames.head)
    var fieldVal: Long = -1
    attribute.getType match {
      case AttributeType.LONG =>
        fieldVal = tuple.getField[Long](attribute)
      case AttributeType.INTEGER =>
        fieldVal = tuple.getField[Int](attribute)
      case AttributeType.DOUBLE =>
        fieldVal = tuple.getField[Double](attribute).toLong
      case _ =>
        throw new RuntimeException(s"unsupported attribute type: ${attribute.getType}")
    }

    if (fieldVal < partitioning.rangeMin) {
      return Iterator(0)
    }
    if (fieldVal > partitioning.rangeMax) {
      return Iterator(receivers.length - 1)
    }
    Iterator(((fieldVal - partitioning.rangeMin) / keysPerReceiver).toInt)
  }

  override def allReceivers: Seq[ActorVirtualIdentity] = receivers

}
