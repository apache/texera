package edu.uci.ics.amber.engine.architecture.worker.neo

import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.{
  DataEvent,
  DataPayload,
  DummyPayload,
  EndPayload
}
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.ITuple

class BatchToTupleConverter(internalQueue: WorkerInternalQueue) {

  // save current batch related information
  private var currentBatch: DataEvent = _
  private var currentInput = 0
  private var exhaustedFlag = true

  // indicate if all upstreams exhausted
  private var allExhausted = false
  private var inputExhaustedCount = 0

  /** get next input tuple
    * should only be called from dp thread
    * @return tuple
    */
  def getNextInputTuple: Either[ITuple, InputExhausted] = {
    // if batch is unavailable, take one from batchInput and reset cursor
    if (isCurrentBatchExhausted) {
      currentBatch = internalQueue.blockingDeque.take()
    }
    // if the batch is dummy batch inserted by worker, return null to unblock dp thread
    currentBatch match {
      case DataPayload(input, tuples) =>
        // if current batch is a data batch, return tuple
        currentInput = input
        // empty iterators will be filtered in WorkerInternalQueue so we can safely call next()
        Left(tuples.next())
      case EndPayload(input) =>
        // current batch is an End of Data sign.
        inputExhaustedCount += 1
        // check if End of Data sign from every upstream has been received
        allExhausted = internalQueue.inputMap.size == inputExhaustedCount
        currentInput = input
        Right(InputExhausted())
      case DummyPayload() =>
        null
    }
  }

  def getCurrentInput: Int = currentInput

  def isAllUpstreamsExhausted: Boolean = allExhausted

  def isCurrentBatchExhausted: Boolean =
    currentBatch == null || currentBatch.isExhausted
}
