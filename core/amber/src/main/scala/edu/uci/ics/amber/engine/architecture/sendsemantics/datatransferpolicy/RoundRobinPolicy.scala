package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.architecture.sendsemantics.routees.BaseRoutee
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{DataMessage, EndSending}
import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.{Actor, ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class RoundRobinPolicy(batchSize: Int) extends DataTransferPolicy(batchSize) {
  var receivers: Array[ActorRef] = _
  // var sequenceNum: Array[Long] = _
  var roundRobinIndex = 0
  var batch: Array[ITuple] = _
  var currentSize = 0

  override def noMore()(implicit sender: ActorRef): Array[(ActorRef,Array[ITuple])] = {
    if (currentSize > 0) {
//      routees(roundRobinIndex).schedule(
//        DataMessage(sequenceNum(roundRobinIndex), batch.slice(0, currentSize))
//      )
//      sequenceNum(roundRobinIndex) += 1
      return Array[(ActorRef,Array[ITuple])]((receivers(roundRobinIndex), batch.slice(0, currentSize)))
    }
    return Array[(ActorRef,Array[ITuple])]()
  }

  override def addToBatch(tuple: ITuple)(implicit sender: ActorRef): Option[(ActorRef,Array[ITuple])] = {
    batch(currentSize) = tuple
    currentSize += 1
    if (currentSize == batchSize) {
      currentSize = 0
      // routees(roundRobinIndex).schedule(DataMessage(sequenceNum(roundRobinIndex), batch))
      // sequenceNum(roundRobinIndex) += 1
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
    // routees.foreach(_.initialize(tag))
    batch = new Array[ITuple](batchSize)
    // sequenceNum = new Array[Long](routees.length)
  }

  override def reset(): Unit = {
    // routees.foreach(_.reset())
    batch = new Array[ITuple](batchSize)
    // sequenceNum = new Array[Long](routees.length)
    roundRobinIndex = 0
    currentSize = 0
  }
}
