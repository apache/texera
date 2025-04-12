package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.core.marker.{EndOfInputChannel, Marker, StartOfInputChannel}
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{DPInputQueueElement, FIFOMessageElement}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, MarkerFrame, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.util.MATERIALIZATION_READER_ACTOR_PREFIX
import edu.uci.ics.amber.util.VirtualIdentityUtils.getWorkerIndex

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer

// Reader thread that reads tuples from a storage iterator and enqueues them.
class InputPortMaterializationReaderThread(
    materialization: VirtualDocument[Tuple],
    inputMessageQueue: LinkedBlockingQueue[DPInputQueueElement],
    workerActorId: ActorVirtualIdentity,
    batchSize: Int = AmberConfig.defaultDataTransferBatchSize
) extends Thread {

  private val sequenceNum = new AtomicLong()
  private val buffer = new ArrayBuffer[Tuple]()
  private val storageReadIterator = materialization.get()
  private lazy val workerIndex = getWorkerIndex(workerActorId)
  private lazy val channelId = {
    // A unique channel between this thread (dummy actor) and the worker actor.
    val storageURIStr = materialization.getURI.toString
    val fromActorId = ActorVirtualIdentity(MATERIALIZATION_READER_ACTOR_PREFIX + storageURIStr)
    ChannelIdentity(fromActorId, workerActorId, isControl = false)
  }
  // TODO: Add a partitionFilter

  override def run(): Unit = {
    // Notify the input port of start of input channel
    emitMarker(StartOfInputChannel())
    try {
      // Produce tuples
      while (storageReadIterator.hasNext) {
        buffer.append(storageReadIterator.next())
        if (buffer.size >= batchSize) {
          flush()
        }
      }
      // Flush any remaining tuples in the buffer.
      if (buffer.nonEmpty) flush()
    } finally {
      emitMarker(EndOfInputChannel())
      // FinalizeInputPort, FinalizeOutputPort, and FinalizeExecutor will be sent by dp once it receives EndOfInputChannel()
    }
  }

  private def emitMarker(marker: Marker): Unit = {
    flush()
    val markerPayload = MarkerFrame(marker)
    val fifoMessage = WorkflowFIFOMessage(channelId, getSequenceNumber, markerPayload)
    val inputQueueElement = FIFOMessageElement(fifoMessage)
    inputMessageQueue.put(inputQueueElement)
    flush()
  }

  // Flush the current batch into a DataFrame and enqueue it.
  private def flush(): Unit = {
    val dataPayload = DataFrame(buffer.toArray) // Mimics flush logic in NetworkOutputBuffer.
    val fifoMessage = WorkflowFIFOMessage(channelId, getSequenceNumber, dataPayload)
    val inputQueueElement = FIFOMessageElement(fifoMessage)
    inputMessageQueue.put(inputQueueElement)
    buffer.clear()
  }

  private def getSequenceNumber = {
    sequenceNum.getAndIncrement()
  }
}
