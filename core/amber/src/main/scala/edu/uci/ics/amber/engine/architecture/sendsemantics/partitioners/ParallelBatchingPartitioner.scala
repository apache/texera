package edu.uci.ics.amber.engine.architecture.sendsemantics.partitioners

import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable.ArrayBuffer

abstract class ParallelBatchingPartitioner(batchSize: Int, receivers: Seq[ActorVirtualIdentity])
    extends Partitioner {
  var batches: Array[Array[ITuple]] = _
  var currentSizes: Array[Int] = _

  initializeInternalState(receivers)

  def selectBatchingIndex(tuple: ITuple): Int

  override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = {
    val receiversAndBatches = new ArrayBuffer[(ActorVirtualIdentity, DataPayload)]
    for (k <- receivers.indices) {
      if (currentSizes(k) > 0) {
        receiversAndBatches.append(
          (receivers(k), DataFrame(batches(k).slice(0, currentSizes(k))))
        )
      }
      receiversAndBatches.append((receivers(k), EndOfUpstream()))
    }
    receiversAndBatches.toArray
  }

  override def addTupleToBatch(
      tuple: ITuple
  ): Option[(ActorVirtualIdentity, DataPayload)] = {
    val index = selectBatchingIndex(tuple)
    batches(index)(currentSizes(index)) = tuple
    currentSizes(index) += 1
    if (currentSizes(index) == batchSize) {
      currentSizes(index) = 0
      val retBatch = batches(index)
      batches(index) = new Array[ITuple](batchSize)
      return Some((receivers(index), DataFrame(retBatch)))
    }
    None
  }

  override def reset(): Unit = {
    initializeInternalState(receivers)
  }

  private[this] def initializeInternalState(_receivers: Seq[ActorVirtualIdentity]): Unit = {
    batches = new Array[Array[ITuple]](_receivers.length)
    for (i <- _receivers.indices) {
      batches(i) = new Array[ITuple](batchSize)
    }
    currentSizes = new Array[Int](_receivers.length)
  }

}
