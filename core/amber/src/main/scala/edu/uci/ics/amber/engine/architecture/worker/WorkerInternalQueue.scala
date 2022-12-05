package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.logging.{DeterminantLogger, LogManager}
import edu.uci.ics.amber.engine.architecture.recovery.{LocalRecoveryManager, RecoveryQueue}
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

  case class ControlElement(payload: ControlPayload, from: ActorVirtualIdentity)
      extends InternalQueueElement

  case class EndMarker(from: ActorVirtualIdentity) extends InternalQueueElement

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

  def pauseManager: PauseManager
  // logging related variables:
  def logManager: LogManager // require dp thread to have log manager
  def recoveryQueue: RecoveryQueue // require dp thread to have recovery queue
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
    if (recoveryQueue.isReplayCompleted) {
      // may have race condition with restoreInput which happens inside DP thread.
      lock.lock()
      dataQueue.add(elem)
      lock.unlock()
    } else {
      recoveryQueue.add(elem)
    }
  }

  def enqueueCommand(payload: ControlPayload, from: ActorVirtualIdentity): Unit = {
    if (recoveryQueue.isReplayCompleted) {
      // may have race condition with restoreInput which happens inside DP thread.
      lock.lock()
      controlQueue.add(ControlElement(payload, from))
      lock.unlock()
    } else {
      recoveryQueue.add(ControlElement(payload, from))
    }
  }

  def getElement: InternalQueueElement = {
    if (recoveryQueue.isReplayCompleted) {
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
    } else {
      recoveryQueue.get()
    }
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
    recoveryQueue.drainAllStashedElements(dataQueue, controlQueue)
    lock.unlock()
  }

  def isControlQueueNonEmptyOrPaused: Boolean = {
    if (recoveryQueue.isReplayCompleted) {
      determinantLogger.stepIncrement()
      !controlQueue.isEmpty || pauseManager.isPaused()
    } else {
      recoveryQueue.isReadyToEmitNextControl
    }
  }

}
