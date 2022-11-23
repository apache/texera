package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.logging.{DeterminantLogger, LogManager}
import edu.uci.ics.amber.engine.architecture.recovery.LocalRecoveryManager
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{
  CONTROL_QUEUE,
  ControlElement,
  DATA_QUEUE,
  InputTuple,
  InternalQueueElement
}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, DataFrame, EndOfUpstream}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import lbmq.LinkedBlockingMultiQueue

import java.util.concurrent.locks.ReentrantLock
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
  private val lock = new ReentrantLock()

  lbmq.addSubQueue(DATA_QUEUE, DATA_QUEUE)
  lbmq.addSubQueue(CONTROL_QUEUE, CONTROL_QUEUE)

  private val dataQueue = lbmq.getSubQueue(DATA_QUEUE)

  private val controlQueue = lbmq.getSubQueue(CONTROL_QUEUE)

  // logging related variables:
  def logManager: LogManager // require dp thread to have log manager
  def recoveryManager: LocalRecoveryManager // require dp thread to have recovery manager
  protected lazy val determinantLogger: DeterminantLogger = logManager.getDeterminantLogger

  // the values in below maps are in tuples (not batches)
  private var inputTuplesPutInQueue =
    new mutable.HashMap[ActorVirtualIdentity, Long]() // read and written by main thread
  @volatile private var inputTuplesTakenOutOfQueue =
    new mutable.HashMap[ActorVirtualIdentity, Long]() // written by DP thread, read by main thread

  def getSenderCredits(sender: ActorVirtualIdentity): Int = {
    (Constants.unprocessedBatchesCreditLimitPerSender * Constants.defaultBatchSize - (inputTuplesPutInQueue
      .getOrElseUpdate(sender, 0L) - inputTuplesTakenOutOfQueue.getOrElseUpdate(
      sender,
      0L
    )).toInt) / Constants.defaultBatchSize
  }

  def appendElement(elem: InternalQueueElement): Unit = {
    if (Constants.flowControlEnabled) {
      elem match {
        case InputTuple(from, _) =>
          inputTuplesPutInQueue(from) = inputTuplesPutInQueue.getOrElseUpdate(from, 0L) + 1
        case _ =>
        // do nothing
      }
    }
    lock.lock()
    if (recoveryManager.replayCompleted()) {
      dataQueue.add(elem)
    } else {
      recoveryManager.add(elem)
    }
    lock.unlock()
  }

  def enqueueCommand(payload: ControlPayload, from: ActorVirtualIdentity): Unit = {
    lock.lock()
    if (recoveryManager.replayCompleted()) {
      controlQueue.add(ControlElement(payload, from))
    } else {
      recoveryManager.add(ControlElement(payload, from))
    }
    lock.unlock()
  }

  def getElement: InternalQueueElement = {
    val elem = lbmq.take()
    if (Constants.flowControlEnabled) {
      elem match {
        case InputTuple(from, _) =>
          inputTuplesTakenOutOfQueue(from) =
            inputTuplesTakenOutOfQueue.getOrElseUpdate(from, 0L) + 1
        case _ =>
        // do nothing
      }
    }
    elem
  }

  def disableDataQueue(): Unit = {
    if (dataQueue.isEnabled) {
      dataQueue.enable(false)
    }
  }

  def enableDataQueue(): Unit = {
    if (!dataQueue.isEnabled) {
      dataQueue.enable(true)
    }
  }

  def getDataQueueLength: Int = dataQueue.size()

  def getControlQueueLength: Int = controlQueue.size()

  def restoreInputs(): Unit = {
    lock.lock()
    recoveryManager.drainAllStashedElements(dataQueue, controlQueue)
    lock.unlock()
  }

  def isControlQueueEmpty: Boolean = {
    determinantLogger.stepIncrement()
    controlQueue.isEmpty
  }

}
