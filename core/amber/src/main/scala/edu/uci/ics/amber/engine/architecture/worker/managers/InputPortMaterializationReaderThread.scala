package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.core.marker.{EndOfInputChannel, Marker, StartOfInputChannel}
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  FIFOMessageElement
}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, MarkerFrame, WorkflowFIFOMessage}
import edu.uci.ics.amber.util.VirtualIdentityUtils.{
  getFromActorIdForInputPortStorage,
  getWorkerIndex
}

import java.net.URI
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer

// Reader thread that reads tuples from a storage iterator and enqueues them.
class InputPortMaterializationReaderThread(
    uri: URI,
    inputMessageQueue: LinkedBlockingQueue[DPInputQueueElement],
    workerActorId: ActorVirtualIdentity,
    batchSize: Int = AmberConfig.defaultDataTransferBatchSize
) extends Thread {

  private val sequenceNum = new AtomicLong()
  private val buffer = new ArrayBuffer[Tuple]()
  // TODO: Support multi-worker
  private lazy val workerIndex = getWorkerIndex(workerActorId)
  private lazy val channelId = {
    // A unique channel between this thread (dummy actor) and the worker actor.
    val fromActorId: ActorVirtualIdentity = getFromActorIdForInputPortStorage(uri.toString)
    ChannelIdentity(fromActorId, workerActorId, isControl = false)
  }

  override def run(): Unit = {
    // Notify the input port of start of input channel
    emitMarker(StartOfInputChannel())
    try {
      val materialization: VirtualDocument[Tuple] = DocumentFactory
        .openDocument(uri)
        ._1
        .asInstanceOf[VirtualDocument[Tuple]]
      val storageReadIterator = materialization.get()
      // TODO: Support multi-worker
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
    if (buffer.isEmpty) return
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
