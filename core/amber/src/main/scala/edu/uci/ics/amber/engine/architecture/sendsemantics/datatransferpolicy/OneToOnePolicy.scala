package edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy

import edu.uci.ics.amber.engine.architecture.sendsemantics.routees.BaseRoutee
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.{DataMessage, EndSending}
import edu.uci.ics.amber.engine.common.ambertag.LinkTag
import edu.uci.ics.amber.engine.common.tuple.ITuple
import akka.actor.{Actor, ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class OneToOnePolicy(batchSize: Int) extends DataTransferPolicy(batchSize) {
  var sequenceNum: Long = 0
  var routee: BaseRoutee = _
  var batch: Array[ITuple] = _
  var currentSize = 0
  override def accept(tuple: ITuple)(implicit sender: ActorRef): Unit = {
    batch(currentSize) = tuple
    currentSize += 1
    if (currentSize == batchSize) {
      currentSize = 0
      routee.schedule(DataMessage(sequenceNum, batch))
      sequenceNum += 1
      batch = new Array[ITuple](batchSize)
    }
  }

  override def noMore()(implicit sender: ActorRef): Unit = {
    if (currentSize > 0) {
      routee.schedule(DataMessage(sequenceNum, batch.slice(0, currentSize)))
      sequenceNum += 1
    }
    routee.schedule(EndSending(sequenceNum))
  }

  override def pause(): Unit = {
    routee.pause()
  }

  override def resume()(implicit sender: ActorRef): Unit = {
    routee.resume()
  }

  override def initialize(tag: LinkTag, next: Array[BaseRoutee])(implicit
      ac: ActorContext,
      sender: ActorRef,
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter
  ): Unit = {
    super.initialize(tag, next)
    assert(next != null && next.length == 1)
    routee = next(0)
    routee.initialize(tag)
    batch = new Array[ITuple](batchSize)
  }

  override def dispose(): Unit = {
    routee.dispose()
  }

  override def reset(): Unit = {
    routee.reset()
    batch = new Array[ITuple](batchSize)
    currentSize = 0
    sequenceNum = 0L
  }
}
