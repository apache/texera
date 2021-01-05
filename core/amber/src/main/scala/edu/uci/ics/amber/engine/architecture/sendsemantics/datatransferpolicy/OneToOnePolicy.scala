package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{DataFrame, EndOfUpstream}
import edu.uci.ics.amber.engine.common.ambermessage.neo.DataPayload
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

import scala.collection.mutable.ArrayBuffer

class OneToOnePolicy(batchSize: Int) extends DataTransferPolicy(batchSize) {
  var receiver: ActorVirtualIdentity = _
  var batch: Array[ITuple] = _
  var currentSize = 0

  override def addTupleToBatch(
      tuple: ITuple
  ): Option[(ActorVirtualIdentity, DataPayload)] = {
    batch(currentSize) = tuple
    currentSize += 1
    if (currentSize == batchSize) {
      currentSize = 0
      val retBatch = batch
      batch = new Array[ITuple](batchSize)
      return Some((receiver, DataFrame(retBatch)))
    }
    None
  }

  override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = {
    val ret = new ArrayBuffer[(ActorVirtualIdentity, DataPayload)]
    if (currentSize > 0) {
      ret.append((receiver, DataFrame(batch.slice(0, currentSize))))
    }
    ret.append((receiver, EndOfUpstream()))
    ret.toArray
  }

  override def initialize(tag: LinkTag, _receivers: Array[ActorVirtualIdentity]): Unit = {
    super.initialize(tag, _receivers)
    assert(_receivers != null && _receivers.length == 1)
    receiver = _receivers(0)
    batch = new Array[ITuple](batchSize)
  }

  override def reset(): Unit = {
    batch = new Array[ITuple](batchSize)
    currentSize = 0
  }
}
