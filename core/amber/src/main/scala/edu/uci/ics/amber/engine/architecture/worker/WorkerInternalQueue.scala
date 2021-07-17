package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{
  CONTROL_QUEUE,
  ControlElement,
  DATA_QUEUE,
  InternalQueueElement
}
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayload
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import lbmq.LinkedBlockingMultiQueue

object WorkerInternalQueue {
  final val DATA_QUEUE = 1
  final val CONTROL_QUEUE = 0

  // 4 kinds of elements can be accepted by internal queue
  sealed trait InternalQueueElement

  case class InputTuple(tuple: ITuple) extends InternalQueueElement

  case class SenderChangeMarker(newUpstreamLink: LinkIdentity) extends InternalQueueElement

  case class ControlElement(cmd: ControlPayload, from: ActorVirtualIdentity)
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

  def appendElement(elem: InternalQueueElement): Unit = {
    dataQueue.add(elem)
  }

  def enqueueCommand(cmd: ControlPayload, from: ActorVirtualIdentity): Unit = {
    controlQueue.add(ControlElement(cmd, from))
  }

  def getElement: InternalQueueElement = lbmq.take()

  def disableDataQueue(): Unit = dataQueue.enable(false)

  def enableDataQueue(): Unit = dataQueue.enable(true)

  def getDataQueueLength: Int = dataQueue.size()

  def getControlQueueLength: Int = controlQueue.size()

  def isControlQueueEmpty: Boolean = controlQueue.isEmpty

}
