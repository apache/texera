package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.{ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class OneToOnePolicy(batchSize: Int) extends DataTransferPolicy(batchSize) {
  var receiver: ActorRef = _
  var batch: Array[ITuple] = _
  var currentSize = 0

  override def addTupleToBatch(
      tuple: ITuple
  )(implicit sender: ActorRef): Option[(ActorRef, Array[ITuple])] = {
    batch(currentSize) = tuple
    currentSize += 1
    if (currentSize == batchSize) {
      currentSize = 0
      val retBatch = batch
      batch = new Array[ITuple](batchSize)
      return Some((receiver, retBatch))
    }
    None
  }

  override def noMore()(implicit sender: ActorRef): Array[(ActorRef, Array[ITuple])] = {
    if (currentSize > 0) {
      return Array[(ActorRef, Array[ITuple])]((receiver, batch.slice(0, currentSize)))
    }
    return Array[(ActorRef, Array[ITuple])]()
  }

  override def initialize(tag: LinkTag, _receivers: Array[ActorRef])(implicit
      ac: ActorContext,
      sender: ActorRef,
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
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
