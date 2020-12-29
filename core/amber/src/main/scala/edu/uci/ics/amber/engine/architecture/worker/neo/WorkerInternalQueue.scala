package edu.uci.ics.amber.engine.architecture.worker.neo

import java.util.concurrent.LinkedBlockingDeque
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.{
  DummyInput,
  EndMarker,
  InputTuple,
  InternalQueueElement,
  SenderChangeMarker
}
import edu.uci.ics.amber.engine.common.{InputExhausted, WorkflowLogger}
import edu.uci.ics.amber.engine.common.ambertag.LayerTag
import edu.uci.ics.amber.engine.common.tuple.ITuple

import scala.collection.mutable
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage.LogErrorToFrontEnd
import edu.uci.ics.amber.error.WorkflowRuntimeError

object WorkerInternalQueue {

  // 4 kinds of elements can be accepted by internal queue
  trait InternalQueueElement {}

  case class InputTuple(tuple: ITuple) extends InternalQueueElement {}
  case class SenderChangeMarker(newSenderRef: Int) extends InternalQueueElement {}
  case class EndMarker(input: Int) extends InternalQueueElement {}

  /**
    * Used to unblock the dp thread when pause arrives but
    * dp thread is blocked waiting for the next element in the
    * worker-internal-queue
    */
  case class DummyInput() extends InternalQueueElement {}
}

class WorkerInternalQueue {
  // blocking deque for batches:
  // main thread put batches into this queue
  // tuple input (dp thread) take batches from this queue
  private var blockingDeque = new LinkedBlockingDeque[InternalQueueElement]

  private def errorLogAction(err: WorkflowRuntimeError): Unit = {
    Logger("WorkerInternalQueue").error(err.convertToMap().mkString(" | "))
  }
  val errorLogger = WorkflowLogger(errorLogAction)

  /**
    * map from layerTag to input number. Used to convert the LayerTag
    * to int when adding sender info to the queue.
    */
  // TODO: we also need to refactor all identifiers
  var inputMap = new mutable.HashMap[LayerTag, Int]

  /**
    * Used to remember which sender the InputTuple(s) incoming into queue are from. This information
    * is passed to DP thread. Gets updated when a SenderChangeMarker() comes.
    */
  private var currentSenderAtQueueInput: Int = -1
  private var currentSenderAtQueueOutput: Int = -1

  // indicate if all upstreams exhausted
  private var allExhausted = false
  private var inputExhaustedCount = 0

  def isQueueEmpty(): Boolean = {
    blockingDeque.isEmpty
  }

  /** take one sender and tuple from main thread and put in the queue.
    * To save memory, we put sender in queue only when it gets changed.
    * @param batch
    */
  def addSenderTuplePair(dataPair: (LayerTag, ITuple)): Unit = {
    if (dataPair == null || dataPair._2 == null) {
      // also filter out the batch with no tuple here
      return
    }

    if (inputMap(dataPair._1) != currentSenderAtQueueInput) {
      currentSenderAtQueueInput = inputMap(dataPair._1)
      blockingDeque.add(SenderChangeMarker(currentSenderAtQueueInput))
    }
    blockingDeque.add(InputTuple(dataPair._2))
  }

  /** put an end batch into the queue.
    * @param layer
    */
  def addEndMarker(layer: LayerTag): Unit = {
    if (layer == null) {
      return
    }
    blockingDeque.add(EndMarker(inputMap(layer)))
  }

  /** put an dummy batch into the queue to unblock the dp thread.
    */
  def addDummyInput(): Unit = {
    blockingDeque.add(DummyInput())
  }

  private def isSenderChangeMarker(elem: InternalQueueElement): Boolean = {
    elem match {
      case SenderChangeMarker(newSenderRef) =>
        return true
      case _ =>
        return false
    }
  }

  /** get next input tuple
    * should only be called from dp thread
    * @return tuple
    */
  def getNextInputPair: (Int, Either[ITuple, InputExhausted]) = {

    var currentInput = blockingDeque.take()
    while (isSenderChangeMarker(currentInput)) {
      // this should be the first element in the queue before any tuple gets in
      currentSenderAtQueueOutput = currentInput.asInstanceOf[SenderChangeMarker].newSenderRef
      currentInput = blockingDeque.take()
    }

    currentInput match {
      case InputTuple(tuple) =>
        // empty iterators will be filtered in WorkerInternalQueue so we can safely call next()
        if (currentSenderAtQueueOutput == -1) {
          val error = WorkflowRuntimeError(
            "Sender still -1 at queue output",
            "WorkerInternalQueue",
            Map()
          )
          errorLogger.log(error)
        }
        (currentSenderAtQueueOutput, Left(tuple))
      case EndMarker(senderRef) =>
        // current batch is an End of Data sign.
        inputExhaustedCount += 1
        // check if End of Data sign from every upstream has been received
        allExhausted = (inputMap.size == inputExhaustedCount)
        (senderRef, Right(InputExhausted()))
      case DummyInput() =>
        // if the batch is dummy batch inserted by worker, return null to unblock dp thread
        (-1, null)
    }
  }

  def isAllUpstreamsExhausted: Boolean = allExhausted
}
