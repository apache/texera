package edu.uci.ics.amber.engine.architecture.worker.neo

import java.util.concurrent.LinkedBlockingDeque
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.{
  InternalQueueElement,
  DataPayload,
  DummyInput,
  EndMarker
}
import edu.uci.ics.amber.engine.common.ambertag.LayerTag
import edu.uci.ics.amber.engine.common.tuple.ITuple

import scala.collection.mutable

object WorkerInternalQueue {
  // 3 kinds of data batches can be accepted by internal queue
  trait InternalQueueElement {
    // indicate if the batch is exhausted so dp thread can fetch the next batch
    def isExhausted: Boolean
  }
  // data batch with the input as a number and an iterator of tuples (guaranteed to be non-empty)
  case class DataPayload(input: Int, tuples: Iterator[ITuple]) extends InternalQueueElement {
    // when the iterator has no next, it's exhausted.
    // Note: since the iterator is guaranteed to be non-empty,
    // the first call of isExhausted will return false.
    override def isExhausted: Boolean = !tuples.hasNext
  }
  case class EndMarker(input: Int) extends InternalQueueElement {
    // this will only be used once so it's always exhausted
    override def isExhausted: Boolean = true
  }
  case class DummyInput() extends InternalQueueElement {
    // this will only be used once so it's always exhausted
    override def isExhausted: Boolean = true
  }
}

class WorkerInternalQueue {
  // blocking deque for batches:
  // main thread put batches into this queue
  // tuple input (dp thread) take batches from this queue
  var blockingDeque = new LinkedBlockingDeque[InternalQueueElement]

  // map from layerTag to input number
  // TODO: we also need to refactor all identifiers
  var inputMap = new mutable.HashMap[LayerTag, Int]

  /** take one FIFO batch from worker actor then put into the queue.
    * @param batch
    */
  def addDataBatch(batch: (LayerTag, Array[ITuple])): Unit = {
    if (batch == null || batch._2.isEmpty) {
      // also filter out the batch with no tuple here
      return
    }
    blockingDeque.add(DataPayload(inputMap(batch._1), batch._2.iterator))
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
}
