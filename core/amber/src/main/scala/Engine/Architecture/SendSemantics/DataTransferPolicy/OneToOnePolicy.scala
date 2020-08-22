package Engine.Architecture.SendSemantics.DataTransferPolicy

import Engine.Architecture.SendSemantics.Routees.BaseRoutee
import Engine.Common.AmberMessage.WorkerMessage.{DataMessage, EndSending}
import Engine.Common.AmberTag.LinkTag
import Engine.Common.AmberTuple.Tuple
import akka.actor.{Actor, ActorContext, ActorRef}
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.concurrent.ExecutionContext

class OneToOnePolicy(batchSize: Int) extends DataTransferPolicy(batchSize) {
  var sequenceNum: Long = 0
  var routee: BaseRoutee = _
  var batch: Array[Tuple] = _
  var currentSize = 0
  override def accept(tuple: Tuple)(implicit sender: ActorRef): Unit = {
    batch(currentSize) = tuple
    currentSize += 1
    if (currentSize == batchSize) {
      currentSize = 0
      routee.schedule(DataMessage(sequenceNum, batch))
      sequenceNum += 1
      batch = new Array[Tuple](batchSize)
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
    batch = new Array[Tuple](batchSize)
  }

  override def dispose(): Unit = {
    routee.dispose()
  }

  override def reset(): Unit = {
    routee.reset()
    batch = new Array[Tuple](batchSize)
    currentSize = 0
    sequenceNum = 0L
  }
}
