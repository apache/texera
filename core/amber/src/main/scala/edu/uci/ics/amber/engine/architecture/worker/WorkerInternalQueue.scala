package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.logging.{DeterminantLogger, LogManager}
import edu.uci.ics.amber.engine.architecture.recovery.RecoveryQueue
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue._
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, EpochMarker}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import lbmq.LinkedBlockingMultiQueue

import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

object WorkerInternalQueue {
  final val DATA_QUEUE_PRIORITY = 1

  final val CONTROL_QUEUE_KEY = "CONTROL_QUEUE"
  final val CONTROL_QUEUE_PRIORITY = 0

  // 4 kinds of elements can be accepted by internal queue
  sealed trait InternalQueueElement {
    def from: ActorVirtualIdentity
  }

  case class InputTuple(from: ActorVirtualIdentity, tuple: ITuple) extends InternalQueueElement

  case class ControlElement(payload: ControlPayload, from: ActorVirtualIdentity)
      extends InternalQueueElement

  case class EndMarker(from: ActorVirtualIdentity) extends InternalQueueElement

  case class InputEpochMarker(from: ActorVirtualIdentity, epochMarker: EpochMarker)
      extends InternalQueueElement

}

/** Inspired by the mailbox-ed thread, the internal queue should
  * be a part of DP thread.
  */
class WorkerInternalQueue(
    val pauseManager: PauseManager,
    val logManager: LogManager,
    val recoveryQueue: RecoveryQueue
) {

  private[architecture] val lbmq = new LinkedBlockingMultiQueue[String, InternalQueueElement]()
  private[architecture] val lock = new ReentrantLock()

  lbmq.addSubQueue(CONTROL_QUEUE_KEY, CONTROL_QUEUE_PRIORITY)

  private[architecture] val dataQueues =
    new mutable.HashMap[String, LinkedBlockingMultiQueue[String, InternalQueueElement]#SubQueue]()
  private[architecture] val controlQueue = lbmq.getSubQueue(CONTROL_QUEUE_KEY)

  protected lazy val determinantLogger: DeterminantLogger = logManager.getDeterminantLogger

  // the values in below maps are in tuples (not batches)
  private var inputTuplesPutInQueue =
    new mutable.HashMap[ActorVirtualIdentity, Long]() // read and written by main thread
  @volatile private var inputTuplesTakenOutOfQueue =
    new mutable.HashMap[ActorVirtualIdentity, Long]() // written by DP thread, read by main thread

  def registerInput(sender: ActorVirtualIdentity): Unit = {
    lbmq.addSubQueue(sender.name, DATA_QUEUE_PRIORITY)
    dataQueues(sender.name) = lbmq.getSubQueue(sender.name)
  }

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
      if (elem == null || elem.from == null || elem.from.name == null) {
        throw new RuntimeException("from is null for element " + elem)
      }
      if (dataQueues(elem.from.name) == null) {
        throw new RuntimeException(elem.from.name + " actor not registered")
      }
      dataQueues(elem.from.name).add(elem)
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

  def getDataQueueLength: Int = dataQueues.values.map(q => q.size()).sum

  def getControlQueueLength: Int = controlQueue.size()

  def restoreInputs(): Unit = {
    lock.lock()
    recoveryQueue.drainAllStashedElements(this)
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
