package edu.uci.ics.amber.engine.architecture.worker

import java.util.concurrent.LinkedBlockingDeque

import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.InternalQueueElement
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity

object WorkerInternalQueue {
  // 4 kinds of elements can be accepted by internal queue
  trait InternalQueueElement

  //TODO: check if this is creating overhead
  case class InputTuple(tuple: ITuple) extends InternalQueueElement
  case class SenderChangeMarker(newUpstreamLink: LinkIdentity) extends InternalQueueElement
  case class EndMarker() extends InternalQueueElement
  case class EndOfAllMarker() extends InternalQueueElement

  /**
    * Used to unblock the dp thread when pause arrives but
    * dp thread is blocked waiting for the next element in the
    * worker-internal-queue
    */
  case class DummyInput() extends InternalQueueElement
}

/** Inspired by the mailbox-ed thread, the internal queue should
  * be a part of DP thread.
  */
trait WorkerInternalQueue {
  // blocking deque for batches:
  // main thread put batches into this queue
  // tuple input (dp thread) take batches from this queue
  protected val blockingDeque = new LinkedBlockingDeque[InternalQueueElement]

  def isQueueEmpty: Boolean = blockingDeque.isEmpty

  def appendElement(elem: InternalQueueElement): Unit = {
    blockingDeque.add(elem)
  }

  /* called when user want to fix/resume current tuple */
  def prependElement(elem: InternalQueueElement): Unit = {
    blockingDeque.addFirst(elem)
  }

}
