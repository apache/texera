package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity

import scala.collection.mutable.ArrayBuffer

abstract class ParallelBatchingPolicyExec
    extends DataSendingPolicyExec {
  var batches: Array[Array[ITuple]] = _
  var currentSizes: Array[Int] = _

  initializeInternalState(policy.receivers)

  def selectBatchingIndex(tuple: ITuple): Int

  override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = {
    val receiversAndBatches = new ArrayBuffer[(ActorVirtualIdentity, DataPayload)]
    for (k <- policy.receivers.indices) {
      if (currentSizes(k) > 0) {
        receiversAndBatches.append(
          (policy.receivers(k), DataFrame(batches(k).slice(0, currentSizes(k))))
        )
      }
      receiversAndBatches.append((policy.receivers(k), EndOfUpstream()))
    }
    receiversAndBatches.toArray
  }

  override def addTupleToBatch(
      tuple: ITuple
  ): Option[(ActorVirtualIdentity, DataPayload)] = {
    val index = selectBatchingIndex(tuple)
    batches(index)(currentSizes(index)) = tuple
    currentSizes(index) += 1
    if (currentSizes(index) == policy.batchSize) {
      currentSizes(index) = 0
      val retBatch = batches(index)
      batches(index) = new Array[ITuple](policy.batchSize)
      return Some((policy.receivers(index), DataFrame(retBatch)))
    }
    None
  }

  override def reset(): Unit = {
    initializeInternalState(policy.receivers)
  }

  private[this] def initializeInternalState(_receivers: Array[ActorVirtualIdentity]): Unit = {
    batches = new Array[Array[ITuple]](_receivers.length)
    for (i <- _receivers.indices) {
      batches(i) = new Array[ITuple](policy.batchSize)
    }
    currentSizes = new Array[Int](_receivers.length)
  }

}
