package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable.ArrayBuffer

abstract class ParallelBatchingPartitioner extends Partitioner {
  var batches: Array[Array[ITuple]] = _
  var currentSizes: Array[Int] = _

  initializeInternalState(partitioning.receivers)

  def selectBatchingIndex(tuple: ITuple): Int

  override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = {
    val receiversAndBatches = new ArrayBuffer[(ActorVirtualIdentity, DataPayload)]
    for (k <- partitioning.receivers.indices) {
      if (currentSizes(k) > 0) {
        receiversAndBatches.append(
          (partitioning.receivers(k), DataFrame(batches(k).slice(0, currentSizes(k))))
        )
      }
      receiversAndBatches.append((partitioning.receivers(k), EndOfUpstream()))
    }
    receiversAndBatches.toArray
  }

  override def addTupleToBatch(
      tuple: ITuple
  ): Option[(ActorVirtualIdentity, DataPayload)] = {
    val index = selectBatchingIndex(tuple)
    batches(index)(currentSizes(index)) = tuple
    currentSizes(index) += 1
    if (currentSizes(index) == partitioning.batchSize) {
      currentSizes(index) = 0
      val retBatch = batches(index)
      batches(index) = new Array[ITuple](partitioning.batchSize)
      return Some((partitioning.receivers(index), DataFrame(retBatch)))
    }
    None
  }

  override def reset(): Unit = {
    initializeInternalState(partitioning.receivers)
  }

  private[this] def initializeInternalState(_receivers: Array[ActorVirtualIdentity]): Unit = {
    batches = new Array[Array[ITuple]](_receivers.length)
    for (i <- _receivers.indices) {
      batches(i) = new Array[ITuple](partitioning.batchSize)
    }
    currentSizes = new Array[Int](_receivers.length)
  }

}
