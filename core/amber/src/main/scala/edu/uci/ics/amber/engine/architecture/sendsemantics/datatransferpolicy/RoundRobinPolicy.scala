package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.{ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class RoundRobinPolicy(batchSize: Int) extends DataTransferPolicy(batchSize) {
  var receivers: Array[ActorRef] = _
  var roundRobinIndex = 0
  var batch: Array[ITuple] = _
  var currentSize = 0

  override def noMore()(implicit sender: ActorRef): Array[(ActorRef, Array[ITuple])] = {
    if (currentSize > 0) {
      return Array[(ActorRef, Array[ITuple])](
        (receivers(roundRobinIndex), batch.slice(0, currentSize))
      )
    }
    return Array[(ActorRef, Array[ITuple])]()
  }

  override def addToBatch(
      tuple: ITuple
  )(implicit sender: ActorRef): Option[(ActorRef, Array[ITuple])] = {
    batch(currentSize) = tuple
    currentSize += 1
    if (currentSize == batchSize) {
      currentSize = 0
      val retBatch = batch
      roundRobinIndex = (roundRobinIndex + 1) % receivers.length
      batch = new Array[ITuple](batchSize)
      return Some((receivers(roundRobinIndex), retBatch))
    }
    None
  }

  override def initialize(tag: LinkTag, _receivers: Array[ActorRef])(implicit
      ac: ActorContext,
      sender: ActorRef,
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
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
