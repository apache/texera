package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.DPInputQueueElement
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.ambermessage.DataFrame

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer

// Reader thread that reads tuples from a storage iterator and enqueues them.
class InputPortMaterializationReaderThread(
    storageReadIterator: Iterator[Tuple],
    inputMessageQueue: LinkedBlockingQueue[DPInputQueueElement],
    batchSize: Int = AmberConfig.defaultDataTransferBatchSize
) extends Thread {

  // TODO: use inputMessageQueue

  override def run(): Unit = {
    // Buffer to accumulate tuples.
    val buffer = new ArrayBuffer[Tuple]()
    try {
      while (storageReadIterator.hasNext) {
        buffer.append(storageReadIterator.next())
        if (buffer.size >= batchSize) {
          flush(buffer)
        }
      }
      // Flush any remaining tuples in the buffer.
      if (buffer.nonEmpty) flush(buffer)
    } finally {
      // Signal termination once all tuples have been processed.
      inputMessageQueue.put()
    }
  }

  // Flush the current batch into a DataFrame and enqueue it.
  private def flush(buffer: ArrayBuffer[Tuple]): Unit = {
    val batch = DataFrame(buffer.toArray) // Mimics flush logic in NetworkOutputBuffer.
    inputMessageQueue.put(Left(batch))
    buffer.clear()
  }
}
