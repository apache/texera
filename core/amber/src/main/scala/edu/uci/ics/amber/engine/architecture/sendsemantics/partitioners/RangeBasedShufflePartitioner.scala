package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.RangeBasedShufflePartitioning
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType

case class RangeBasedShufflePartitioner(partitioning: RangeBasedShufflePartitioning)
    extends ParallelBatchingPartitioner(partitioning.batchSize, partitioning.receivers) {

  val keysPerReceiver =
    ((partitioning.rangeMax - partitioning.rangeMin) / partitioning.receivers.length).toLong + 1

  override def selectBatchingIndex(tuple: ITuple): Int = {
    // Do range partitioning only on the first attribute in `rangeColumnIndices`.
    val fieldType = tuple
      .asInstanceOf[Tuple]
      .getSchema
      .getAttributes()
      .get(partitioning.rangeColumnIndices(0))
      .getType
    var fieldVal: Long = -1
    fieldType match {
      case AttributeType.LONG =>
        fieldVal = tuple.getLong(partitioning.rangeColumnIndices(0))
      case AttributeType.INTEGER =>
        fieldVal = tuple.getInt(partitioning.rangeColumnIndices(0)).toLong
      case AttributeType.DOUBLE =>
        fieldVal = tuple.getDouble(partitioning.rangeColumnIndices(0)).toLong
      case _ =>
        throw new RuntimeException("unsupported attribute type: " + fieldType.toString())
    }

    if (fieldVal < partitioning.rangeMin) {
      return 0
    }
    if (fieldVal > partitioning.rangeMax) {
      return partitioning.receivers.length - 1
    }
    ((fieldVal - partitioning.rangeMin) / keysPerReceiver).toInt
  }

}
