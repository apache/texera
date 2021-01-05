package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.{ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{DataFrame, EndOfUpstream}
import edu.uci.ics.amber.engine.common.ambermessage.neo.DataPayload
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class RoundRobinPolicy(batchSize: Int) extends DataTransferPolicy(batchSize) {
  var receivers: Array[ActorVirtualIdentity] = _
  var roundRobinIndex = 0
  var batch: Array[ITuple] = _
  var currentSize = 0

  override def noMore(): Array[(ActorVirtualIdentity, DataPayload)] = {
    val ret = new ArrayBuffer[(ActorVirtualIdentity, DataPayload)]
    if (currentSize > 0) {
      ret.append((receivers(roundRobinIndex), DataFrame(batch.slice(0, currentSize))))
    }
    ret.append((receivers(roundRobinIndex), EndOfUpstream()))
    ret.toArray
  }

  override def addTupleToBatch(
      tuple: ITuple
  ): Option[(ActorVirtualIdentity, DataPayload)] = {
    batch(currentSize) = tuple
    currentSize += 1
    if (currentSize == batchSize) {
      currentSize = 0
      val retBatch = batch
      roundRobinIndex = (roundRobinIndex + 1) % receivers.length
      batch = new Array[ITuple](batchSize)
      return Some((receivers(roundRobinIndex), DataFrame(retBatch)))
    }
    None
  }

  override def initialize(tag: LinkTag, _receivers: Array[ActorVirtualIdentity]): Unit = {
    super.initialize(tag, _receivers)
    assert(_receivers != null)
    this.receivers = _receivers
    batch = new Array[ITuple](batchSize)
  }

  override def reset(): Unit = {
    batch = new Array[ITuple](batchSize)
    roundRobinIndex = 0
    currentSize = 0
  }
}
