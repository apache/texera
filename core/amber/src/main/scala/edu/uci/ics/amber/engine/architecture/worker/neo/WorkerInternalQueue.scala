package edu.uci.ics.amber.engine.architecture.worker.neo

import java.util.concurrent.LinkedBlockingDeque

import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.{
  DataEvent,
  DataPayload,
  DummyPayload,
  EndPayload
}
import edu.uci.ics.amber.engine.common.ambertag.LayerTag
import edu.uci.ics.amber.engine.common.tuple.ITuple

import scala.collection.mutable

object WorkerInternalQueue {
  trait DataEvent {
    def isExhausted: Boolean
  }
  case class DataPayload(input: Int, tuples: Iterator[ITuple]) extends DataEvent {
    override def isExhausted: Boolean = !tuples.hasNext
  }
  case class EndPayload(input: Int) extends DataEvent {
    override def isExhausted: Boolean = true
  }
  case class DummyPayload() extends DataEvent {
    override def isExhausted: Boolean = true
  }
}

class WorkerInternalQueue {
  // blocking deque for batches:
  // main thread put batches into this queue
  // tuple input (dp thread) take batches from this queue
  var blockingDeque = new LinkedBlockingDeque[DataEvent]

  // map from layerTag to input number
  // TODO: we also need to refactor all identifiers
  var inputMap = new mutable.HashMap[LayerTag, Int]

  /** take one FIFO batch from worker actor then put into the queue.
    * @param batch
    */
  def addDataBatch(batch: (LayerTag, Array[ITuple])): Unit = {
    if (batch == null || batch._2.isEmpty) {
      return
    }
    blockingDeque.add(DataPayload(inputMap(batch._1), batch._2.iterator))
  }

  def addEndBatch(layer: LayerTag): Unit = {
    if (layer == null) {
      return
    }
    blockingDeque.add(EndPayload(inputMap(layer)))
  }

  def addDummyBatch(): Unit = {
    blockingDeque.add(DummyPayload())
  }
}
