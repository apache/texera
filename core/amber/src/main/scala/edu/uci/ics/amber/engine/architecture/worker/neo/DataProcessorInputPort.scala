package edu.uci.ics.amber.engine.architecture.worker.neo

import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.{
  DummyInput,
  EndMarker,
  InternalQueueElement,
  SenderTuplePair
}
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.ITuple

class DataProcessorInputPort(internalQueue: WorkerInternalQueue) {

  // indicate if all upstreams exhausted
  private var allExhausted = false
  private var inputExhaustedCount = 0

  /** get next input tuple
    * should only be called from dp thread
    * @return tuple
    */
  def getNextInputPair: (Int, Either[ITuple, InputExhausted]) = {

    val currentInput = internalQueue.blockingDeque.take()

    currentInput match {
      case SenderTuplePair(senderRef, tuple) =>
        // if current batch is a data batch, return tuple
        // empty iterators will be filtered in WorkerInternalQueue so we can safely call next()
        (senderRef, Left(tuple))
      case EndMarker(senderRef) =>
        // current batch is an End of Data sign.
        inputExhaustedCount += 1
        // check if End of Data sign from every upstream has been received
        allExhausted = internalQueue.inputMap.size == inputExhaustedCount
        (senderRef, Right(InputExhausted()))
      case DummyInput() =>
        // if the batch is dummy batch inserted by worker, return null to unblock dp thread
        (-1, null)
    }
  }

  def isAllUpstreamsExhausted: Boolean = allExhausted

}
