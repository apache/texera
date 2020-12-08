package edu.uci.ics.amber.engine.architecture.worker.neo

import java.util.concurrent.LinkedBlockingDeque

import edu.uci.ics.amber.engine.common.ambertag.LayerTag
import edu.uci.ics.amber.engine.common.tuple.ITuple

import scala.collection.mutable

class WorkerInternalQueue {
  // blocking deque for batches:
  // main thread put batches into this queue
  // tuple input (dp thread) take batches from this queue
  var blockingDeque = new LinkedBlockingDeque[(Int, Array[ITuple])]

  // map from layerTag to input number
  // TODO: we also need to refactor all identifiers
  var inputMap = new mutable.HashMap[LayerTag, Int]

  /** take one FIFO batch from worker actor then put into the queue.
    * @param batch
    */
  def addBatch(batch: (LayerTag, Array[ITuple])): Unit = {
    blockingDeque.add((inputMap(batch._1), batch._2))
  }
}
