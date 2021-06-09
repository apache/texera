package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, DataPayload, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity

import scala.collection.mutable.ArrayBuffer

class OneToOnePolicy(
    policyTag: LinkIdentity,
    batchSize: Int,
    receivers: Array[ActorVirtualIdentity]
) extends DataSendingPolicy(policyTag, batchSize, receivers) {
  var batch: Array[ITuple] = new Array[ITuple](batchSize)
  var currentSize = 0

  assert(receivers.length == 1)

  override def addTupleToBatch(
      tuple: ITuple
  ): Option[(ActorVirtualIdentity, DataPayload)] = {
    batch(currentSize) = tuple
    currentSize += 1
    if (currentSize == batchSize) {
      currentSize = 0
      val retBatch = batch
      batch = new Array[ITuple](batchSize)
      return Some((receivers(0), DataFrame(retBatch)))
    }
    None
  }

  override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = {
    val ret = new ArrayBuffer[(ActorVirtualIdentity, DataPayload)]
    if (currentSize > 0) {
      ret.append((receivers(0), DataFrame(batch.slice(0, currentSize))))
    }
    ret.append((receivers(0), EndOfUpstream()))
    ret.toArray
  }

  override def reset(): Unit = {
    batch = new Array[ITuple](batchSize)
    currentSize = 0
  }
}
