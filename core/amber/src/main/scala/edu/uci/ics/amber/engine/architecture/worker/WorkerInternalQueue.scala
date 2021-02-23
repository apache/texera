package edu.uci.ics.amber.engine.architecture.worker

import java.util.concurrent.{LinkedBlockingDeque, LinkedBlockingQueue}

import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{
  InternalQueueElement,
  UnblockForControlCommands
}
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayload
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LinkIdentity,
  VirtualIdentity
}

object WorkerInternalQueue {
  // 4 kinds of elements can be accepted by internal queue
  sealed trait InternalQueueElement

  //TODO: check if this is creating overhead
  case class InputTuple(tuple: ITuple) extends InternalQueueElement
  case class SenderChangeMarker(newUpstreamLink: LinkIdentity) extends InternalQueueElement
  case object EndMarker extends InternalQueueElement
  case object EndOfAllMarker extends InternalQueueElement

  /**
    * Used to unblock the dp thread when pause arrives but
    * dp thread is blocked waiting for the next element in the
    * worker-internal-queue
    */
  case object UnblockForControlCommands extends InternalQueueElement
}

/** Inspired by the mailbox-ed thread, the internal queue should
  * be a part of DP thread.
  */
trait WorkerInternalQueue {
  // blocking deque for batches:
  // main thread put batches into this queue
  // tuple input (dp thread) take batches from this queue
  protected val dataDeque = new LinkedBlockingDeque[InternalQueueElement]

  protected val controlQueue = new LinkedBlockingQueue[(ControlPayload, VirtualIdentity)]

  def isDataDequeEmpty: Boolean = dataDeque.isEmpty

  def isControlQueueEmpty: Boolean = controlQueue.isEmpty

  def appendElement(elem: InternalQueueElement): Unit = {
    dataDeque.add(elem)
  }

  /* called when user want to fix/resume current tuple */
  def prependElement(elem: InternalQueueElement): Unit = {
    dataDeque.addFirst(elem)
  }

  def enqueueCommand(cmd: ControlPayload, from: VirtualIdentity): Unit = {
    // this enqueue operation MUST happen before checking data queue.
    controlQueue.add((cmd, from))
    // enqueue a unblock data message if data queue is empty.
    if (isDataDequeEmpty) {
      appendElement(UnblockForControlCommands)
    }
  }

}
