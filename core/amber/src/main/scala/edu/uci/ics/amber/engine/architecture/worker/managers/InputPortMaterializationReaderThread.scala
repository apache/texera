package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.DPInputQueueElement
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.ambermessage.DataFrame

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer

// Termination signal for the reader thread.
sealed trait TerminateSignalInput
case object InputPortMaterializationReaderTerminateSignal extends TerminateSignalInput

// Reader thread that reads tuples from a storage iterator and enqueues them.
class InputPortMaterializationReaderThread(
    storageReadIterator: Iterator[Tuple],
    inputMessageQueue: LinkedBlockingQueue[DPInputQueueElement],
    batchSize: Int = AmberConfig.defaultDataTransferBatchSize
) extends Thread {

  // TODO: use inputMessageQueue
  // Internal queue used to communicate with the calling thread.
  val queue: LinkedBlockingQueue[Either[DataFrame, TerminateSignalInput]] =
    new LinkedBlockingQueue[Either[DataFrame, TerminateSignalInput]]()

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
      queue.put(Right(InputPortMaterializationReaderTerminateSignal))
    }
  }

  // Flush the current batch into a DataFrame and enqueue it.
  private def flush(buffer: ArrayBuffer[Tuple]): Unit = {
    val batch = DataFrame(buffer.toArray) // Mimics flush logic in NetworkOutputBuffer.
    queue.put(Left(batch))
    buffer.clear()
  }
}
