package edu.uci.ics.amber.engine.architecture.worker.neo

import java.util.concurrent.LinkedBlockingDeque
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.{
  DummyInput,
  EndMarker,
  InternalQueueElement,
  SenderTuplePair
}
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.ambertag.LayerTag
import edu.uci.ics.amber.engine.common.tuple.ITuple

import scala.collection.mutable

object WorkerInternalQueue {

  // 3 kinds of data batches can be accepted by internal queue
  trait InternalQueueElement {}

  // TODO: input is going now with all tuples. This may lead to a major increase in size
  /**
    * Data Payload is 'input identifier + data batch'
    * @param input
    * @param tuples
    */
  case class SenderTuplePair(senderRef: Int, tuple: ITuple) extends InternalQueueElement {}
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
  var blockingDeque = new LinkedBlockingDeque[InternalQueueElement]

  // map from layerTag to input number
  // TODO: we also need to refactor all identifiers
  var inputMap = new mutable.HashMap[LayerTag, Int]

  // indicate if all upstreams exhausted
  private var allExhausted = false
  private var inputExhaustedCount = 0

  /** take one FIFO batch from worker actor then put into the queue.
    * @param batch
    */
  def addSenderTuplePair(dataPair: (LayerTag, ITuple)): Unit = {
    if (dataPair == null || dataPair._2 == null) {
      // also filter out the batch with no tuple here
      return
    }
    blockingDeque.add(SenderTuplePair(inputMap(dataPair._1), dataPair._2))
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

  /** get next input tuple
    * should only be called from dp thread
    * @return tuple
    */
  def getNextInputPair: (Int, Either[ITuple, InputExhausted]) = {

    val currentInput = blockingDeque.take()

    currentInput match {
      case SenderTuplePair(senderRef, tuple) =>
        // if current batch is a data batch, return tuple
        // empty iterators will be filtered in WorkerInternalQueue so we can safely call next()
        (senderRef, Left(tuple))
      case EndMarker(senderRef) =>
        // current batch is an End of Data sign.
        inputExhaustedCount += 1
        // check if End of Data sign from every upstream has been received
        allExhausted = inputMap.size == inputExhaustedCount
        (senderRef, Right(InputExhausted()))
      case DummyInput() =>
        // if the batch is dummy batch inserted by worker, return null to unblock dp thread
        (-1, null)
    }
  }

  def isAllUpstreamsExhausted: Boolean = allExhausted
}
