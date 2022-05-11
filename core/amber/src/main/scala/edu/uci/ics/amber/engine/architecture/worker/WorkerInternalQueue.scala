package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{
  CONTROL_QUEUE,
  ControlElement,
  DATA_QUEUE,
  EndMarker,
  EndOfAllMarker,
  InputTuple,
  InternalQueueElement
}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, DataFrame, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import lbmq.LinkedBlockingMultiQueue

import scala.collection.mutable

object WorkerInternalQueue {
  final val DATA_QUEUE = 1
  final val CONTROL_QUEUE = 0

  // 4 kinds of elements can be accepted by internal queue
  sealed trait InternalQueueElement

  case class InputTuple(from: ActorVirtualIdentity, tuple: ITuple) extends InternalQueueElement

  case class SenderChangeMarker(newUpstreamLink: LinkIdentity) extends InternalQueueElement

  case class ControlElement(payload: ControlPayload, from: ActorVirtualIdentity)
      extends InternalQueueElement

  case object EndMarker extends InternalQueueElement

  case object EndOfAllMarker extends InternalQueueElement

}

/** Inspired by the mailbox-ed thread, the internal queue should
  * be a part of DP thread.
  */
trait WorkerInternalQueue {

  private val lbmq = new LinkedBlockingMultiQueue[Int, InternalQueueElement]()

  lbmq.addSubQueue(DATA_QUEUE, DATA_QUEUE)
  lbmq.addSubQueue(CONTROL_QUEUE, CONTROL_QUEUE)

  private val dataQueue = lbmq.getSubQueue(DATA_QUEUE)

  private val controlQueue = lbmq.getSubQueue(CONTROL_QUEUE)

  // the credits in the `inputToCredits` map are in tuples (not batches)
  private var inputToCredits = new mutable.HashMap[ActorVirtualIdentity, Int]()

  def getSenderCredits(sender: ActorVirtualIdentity): Int = {
    if (!inputToCredits.contains(sender)) {
      inputToCredits(sender) =
        Constants.pairWiseUnprocessedBatchesLimit * Constants.defaultBatchSize
    }
    inputToCredits(sender) / Constants.defaultBatchSize
  }

  def appendElement(elem: InternalQueueElement): Unit = {
    elem match {
      case InputTuple(from, _) =>
        if (!inputToCredits.contains(from)) {
          inputToCredits(from) =
            Constants.pairWiseUnprocessedBatchesLimit * Constants.defaultBatchSize
        }
        inputToCredits(from) = inputToCredits(from) - 1
      case _ =>
      // do nothing
    }
    dataQueue.add(elem)
  }

  def enqueueCommand(payload: ControlPayload, from: ActorVirtualIdentity): Unit = {
    controlQueue.add(ControlElement(payload, from))
  }

  def getElement: InternalQueueElement = {
    val elem = lbmq.take()
    elem match {
      case InputTuple(from, _) =>
        if (!inputToCredits.contains(from)) {
          throw new WorkflowRuntimeException(
            s"Sender of tuple being dequeued is not registered for credits $from"
          )
        }
        inputToCredits(from) = inputToCredits(from) + 1
      case _ =>
      // do nothing
    }
    elem
  }

  def disableDataQueue(): Unit = dataQueue.enable(false)

  def enableDataQueue(): Unit = dataQueue.enable(true)

  def getDataQueueLength: Int = dataQueue.size()

  def getControlQueueLength: Int = controlQueue.size()

  def isControlQueueEmpty: Boolean = controlQueue.isEmpty

}
