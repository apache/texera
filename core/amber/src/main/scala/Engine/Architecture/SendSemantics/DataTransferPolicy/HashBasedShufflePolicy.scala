package Engine.Architecture.SendSemantics.DataTransferPolicy

import Engine.Architecture.SendSemantics.Routees.BaseRoutee
import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, EndSending}
import Engine.Common.AmberTag.LinkTag
import Engine.Common.AmberTuple.Tuple
import akka.actor.{Actor, ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class HashBasedShufflePolicy(batchSize: Int, val hashFunc: Tuple => Int)
    extends DataTransferPolicy(batchSize) {
  var routees: Array[BaseRoutee] = _
  var sequenceNum: Array[Long] = _
  var batches: Array[Array[Tuple]] = _
  var currentSizes: Array[Int] = _

  override def noMore()(implicit sender: ActorRef): Unit = {
    for (k <- routees.indices) {
      if (currentSizes(k) > 0) {
        routees(k).schedule(DataMessage(sequenceNum(k), batches(k).slice(0, currentSizes(k))))
        sequenceNum(k) += 1
      }
    }
    var i = 0
    while (i < routees.length) {
      routees(i).schedule(EndSending(sequenceNum(i)))
      i += 1
    }
  }

  override def pause(): Unit = {
    for (i <- routees) {
      i.pause()
    }
  }

  override def resume()(implicit sender: ActorRef): Unit = {
    for (i <- routees) {
      i.resume()
    }
  }

  override def accept(tuple: Tuple)(implicit sender: ActorRef): Unit = {
    val numBuckets = routees.length
    val index = (hashFunc(tuple) % numBuckets + numBuckets) % numBuckets
    batches(index)(currentSizes(index)) = tuple
    currentSizes(index) += 1
    if (currentSizes(index) == batchSize) {
      currentSizes(index) = 0
      routees(index).schedule(DataMessage(sequenceNum(index), batches(index)))
      sequenceNum(index) += 1
      batches(index) = new Array[Tuple](batchSize)
    }
  }

  override def initialize(tag: LinkTag, next: Array[BaseRoutee])(implicit
      ac: ActorContext,
      sender: ActorRef,
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    super.initialize(tag, next)
    assert(next != null)
    routees = next
    routees.foreach(_.initialize(tag))
    batches = new Array[Array[Tuple]](next.length)
    for (i <- next.indices) {
      batches(i) = new Array[Tuple](batchSize)
    }
    currentSizes = new Array[Int](routees.length)
    sequenceNum = new Array[Long](routees.length)
  }

  override def dispose(): Unit = {
    routees.foreach(_.dispose())
  }

  override def reset(): Unit = {
    routees.foreach(_.reset())
    batches = new Array[Array[Tuple]](routees.length)
    for (i <- routees.indices) {
      batches(i) = new Array[Tuple](batchSize)
    }
    currentSizes = new Array[Int](routees.length)
    sequenceNum = new Array[Long](routees.length)
  }
}
