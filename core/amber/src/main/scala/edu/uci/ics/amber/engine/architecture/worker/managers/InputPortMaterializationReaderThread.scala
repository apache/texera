/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.core.marker.{EndOfInputChannel, Marker, StartOfInputChannel}
import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.model.VirtualDocument
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager.toPartitioner
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  FIFOMessageElement
}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, MarkerFrame, WorkflowFIFOMessage}
import edu.uci.ics.amber.util.VirtualIdentityUtils.getFromActorIdForInputPortStorage

import java.net.URI
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ArrayBuffer

class InputPortMaterializationReaderThread(
    uri: URI,
    inputMessageQueue: LinkedBlockingQueue[DPInputQueueElement],
    workerActorId: ActorVirtualIdentity,
    partitioning: Partitioning
) extends Thread {

  private val sequenceNum = new AtomicLong()
  private val buffer = new ArrayBuffer[Tuple]()
  private lazy val channelId = {
    // A unique channel between this thread (dummy actor) and the worker actor.
    val fromActorId: ActorVirtualIdentity =
      getFromActorIdForInputPortStorage(uri.toString, workerActorId)
    ChannelIdentity(fromActorId, workerActorId, isControl = false)
  }
  private val partitioner = toPartitioner(partitioning, workerActorId)
  private val batchSize = AmberConfig.defaultDataTransferBatchSize

  /**
    * Read from the materialization stoage, and mimcs the behavior of an upstream worker's output manager.
    */
  override def run(): Unit = {
    // Notify the input port of start of input channel
    emitMarker(StartOfInputChannel())
    try {
      val materialization: VirtualDocument[Tuple] = DocumentFactory
        .openDocument(uri)
        ._1
        .asInstanceOf[VirtualDocument[Tuple]]
      val storageReadIterator = materialization.get()
      // Produce tuples
      while (storageReadIterator.hasNext) {
        val tuple = storageReadIterator.next()
        if (
          partitioner
            .getBucketIndex(tuple)
            .toList
            .exists(bucketIndex => partitioner.allReceivers(bucketIndex) == workerActorId)
        ) {
          buffer.append(tuple)
          if (buffer.size >= batchSize) {
            flush()
          }
        }
      }
      // Flush any remaining tuples in the buffer.
      if (buffer.nonEmpty) flush()
      emitMarker(EndOfInputChannel())
    } catch {
      case e: Exception =>
        throw new RuntimeException(s"Error reading input port materializations: ${e.getMessage}", e)
    }
  }

  /**
    * Puts a marker into the internal queue.
    */
  private def emitMarker(marker: Marker): Unit = {
    flush()
    val markerPayload = MarkerFrame(marker)
    val fifoMessage = WorkflowFIFOMessage(channelId, getSequenceNumber, markerPayload)
    val inputQueueElement = FIFOMessageElement(fifoMessage)
    inputMessageQueue.put(inputQueueElement)
  }

  /**
    * Flush the current batch into a DataFrame and enqueue it.
    */
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
