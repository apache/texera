package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.BroadcastPartitioning
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable.ArrayBuffer

case class BroadcastPartitioner(partitioning: BroadcastPartitioning) extends Partitioner {
  var batch: Array[ITuple] = new Array[ITuple](partitioning.batchSize)
  var currentSize = 0

  override def addTupleToBatch(
      tuple: ITuple
  ): Iterator[(ActorVirtualIdentity, DataPayload)] = {
    val it = Iterator()
    batch(currentSize) = tuple
    if (currentSize == partitioning.batchSize) {
      currentSize = 0
      val retBatch = batch
      for (i <- partitioning.receivers.indices) {
        it ++ Some(partitioning.receivers(i), DataFrame(retBatch))
      }
    }
    it
  }

  override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = {
    val ret = new ArrayBuffer[(ActorVirtualIdentity, DataPayload)]
    if (currentSize > 0) {
      for (i <- partitioning.receivers.indices) {
        ret.append((partitioning.receivers(i), DataFrame(batch.slice(0, currentSize))))
      }
    }
    for (i <- partitioning.receivers.indices) {
      ret.append((partitioning.receivers(i), EndOfUpstream()))
    }

    ret.toArray
  }

  override def reset(): Unit = {
    batch = new Array[ITuple](partitioning.batchSize)
    currentSize = 0
  }
}
