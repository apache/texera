package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.architecture.sendsemantics.routees.BaseRoutee
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{DataMessage, EndSending}
import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.{Actor, ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class HashBasedShufflePolicy(batchSize: Int, val hashFunc: ITuple => Int)
    extends DataTransferPolicy(batchSize) {
  // var routees: Array[BaseRoutee] = _
  // var sequenceNum: Array[Long] = _
  var batches: Array[Array[ITuple]] = _
  var receivers: Array[ActorRef] = _
  var currentSizes: Array[Int] = _

  override def noMore()(implicit sender: ActorRef): Array[(ActorRef,Array[ITuple])] = {
    var receiversAndBatches = new ArrayBuffer[(ActorRef,Array[ITuple])]
    for (k <- receivers.indices) {
      if (currentSizes(k) > 0) {
        //routees(k).schedule(DataMessage(sequenceNum(k), batches(k).slice(0, currentSizes(k))))
        //sequenceNum(k) += 1
        receiversAndBatches.append((receivers(k), batches(k).slice(0, currentSizes(k))))
      }
    }
    receiversAndBatches.toArray
  }

  override def addToBatch(tuple: ITuple)(implicit sender: ActorRef): Option[(ActorRef,Array[ITuple])] = {
    val numBuckets = receivers.length
    val index = (hashFunc(tuple) % numBuckets + numBuckets) % numBuckets
    batches(index)(currentSizes(index)) = tuple
    currentSizes(index) += 1
    if (currentSizes(index) == batchSize) {
      currentSizes(index) = 0
      // routees(index).schedule(DataMessage(sequenceNum(index), batches(index)))
      // sequenceNum(index) += 1
      val retBatch = batches(index)
      batches(index) = new Array[ITuple](batchSize)
      return Some((receivers(index),retBatch))
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
    batches = new Array[Array[ITuple]](_receivers.length)
    for (i <- _receivers.indices) {
      batches(i) = new Array[ITuple](batchSize)
    }
    currentSizes = new Array[Int](_receivers.length)
    // sequenceNum = new Array[Long](routees.length)
  }

  override def reset(): Unit = {
    // routees.foreach(_.reset())
    batches = new Array[Array[ITuple]](receivers.length)
    for (i <- receivers.indices) {
      batches(i) = new Array[ITuple](batchSize)
    }
    currentSizes = new Array[Int](receivers.length)
    // sequenceNum = new Array[Long](routees.length)
  }
}
