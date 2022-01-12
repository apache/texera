package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable.ArrayBuffer

case class OneToOnePartitioner(partitioning: OneToOnePartitioning) extends Partitioner {
  var batch: Array[ITuple] = new Array[ITuple](partitioning.batchSize)
  var currentSize = 0

  assert(partitioning.receivers.length == 1)

  override def addTupleToBatch(
      tuple: ITuple
  ): Option[(ActorVirtualIdentity, DataPayload)] = {
    batch(currentSize) = tuple
    currentSize += 1
    if (currentSize == partitioning.batchSize) {
      currentSize = 0
      val retBatch = batch
      batch = new Array[ITuple](partitioning.batchSize)
      return Some((partitioning.receivers(0), DataFrame(retBatch)))
    }
    None
  }

  override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = {
    val ret = new ArrayBuffer[(ActorVirtualIdentity, DataPayload)]
    if (currentSize > 0) {
      ret.append((partitioning.receivers(0), DataFrame(batch.slice(0, currentSize))))
    }
    ret.append((partitioning.receivers(0), EndOfUpstream()))
    ret.toArray
  }

  override def reset(): Unit = {
    batch = new Array[ITuple](partitioning.batchSize)
    currentSize = 0
  }
}
