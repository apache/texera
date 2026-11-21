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

package org.apache.texera.amber.engine.architecture.sendsemantics.partitioners

import org.apache.texera.common.config.ApplicationConfig
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import org.apache.texera.amber.engine.common.ambermessage.{
  VectorFrame,
  DataFrame,
  LocalColumnarBatch,
  StateFrame
}
import org.apache.texera.amber.util.ArrowUtils
import org.apache.arrow.memory.BufferAllocator

import scala.collection.mutable.ArrayBuffer

trait Partitioner extends Serializable {
  def getBucketIndex(tuple: Tuple): Iterator[Int]

  def allReceivers: Seq[ActorVirtualIdentity]

  // Columnar-shuffle support: let the columnar path assign buckets from just the key columns (via
  // getBucketIndex, identical to the row path) without decoding every row to a Tuple.

  // Does this partitioner split a batch across receivers (shuffle) vs send the whole batch to each?
  def columnarSplits: Boolean = true

  // Columns getBucketIndex reads (hash/range keys); empty when the bucket ignores row data (round-robin).
  def bucketKeyColumns: Seq[String] = Seq.empty

  // True when the bucket depends on the whole row, so the columnar path builds the full tuple per row.
  def requiresFullTuple: Boolean = false
}

class NetworkOutputBuffer(
    val to: ActorVirtualIdentity,
    val dataOutputPort: NetworkOutputGateway,
    val batchSize: Int = ApplicationConfig.defaultDataTransferBatchSize,
    // Per edge (from the planner): flush as VectorFrame (Arrow) instead of DataFrame. Off by default.
    var columnar: Boolean = false,
    // Allocator for the live Arrow root on a local columnar handoff; null disables that path (rows).
    val columnarAllocator: BufferAllocator = null
) {

  // Whether `to` is co-located (same ActorSystem); None = unknown, treated as remote.
  var localityCheck: () => Option[Boolean] = () => None

  // A columnar edge to a REMOTE (or unknown) receiver, which must be serialized to Arrow bytes
  // (VectorFrame). A co-located columnar receiver skips this and gets a zero-copy live root
  // (LocalColumnarBatch) instead; see flush.
  def remoteColumnar: Boolean = columnar && !localityCheck().getOrElse(false)

  var buffer = new ArrayBuffer[Tuple]()

  def addTuple(tuple: Tuple): Unit = {
    buffer.append(tuple)
    if (buffer.size >= batchSize) {
      flush()
    }
  }

  def sendState(state: State, loopCounter: Long = 0L, loopStartId: String = ""): Unit = {
    flush()
    dataOutputPort.sendTo(to, StateFrame(state, loopCounter, loopStartId))
    flush()
  }

  // Ship an already-serialized Arrow batch; flush buffered tuples first to keep FIFO order.
  def sendVectorFrame(frame: VectorFrame): Unit = {
    flush()
    dataOutputPort.sendTo(to, frame)
  }

  // Ship a prebuilt row batch (DataFrame); flush buffered tuples first to keep FIFO order.
  def sendRows(tuples: Array[Tuple]): Unit = {
    flush()
    if (tuples.nonEmpty) dataOutputPort.sendTo(to, DataFrame(tuples))
  }

  // Hand the receiver a live Arrow root by reference (no serialize); ownership transfers.
  def sendLocalColumnar(batch: LocalColumnarBatch): Unit = {
    flush()
    dataOutputPort.sendTo(to, batch)
  }

  def flush(): Unit = {
    if (buffer.nonEmpty) {
      val batch = buffer.toArray
      if (remoteColumnar) {
        // Serialize the batch to Arrow once; all tuples on a link share the schema.
        val schema = batch(0).getSchema
        dataOutputPort.sendTo(
          to,
          VectorFrame(ArrowUtils.serializeTuples(schema, batch), batch.length, schema)
        )
      } else if (columnar && localityCheck().contains(true) && columnarAllocator != null) {
        // Local columnar: build one live Arrow root and hand it to the co-located receiver.
        val schema = batch(0).getSchema
        val root = ArrowUtils.rootFromTuples(schema, batch, columnarAllocator)
        dataOutputPort.sendTo(to, LocalColumnarBatch(root, batch.length, schema))
      } else {
        dataOutputPort.sendTo(to, DataFrame(batch))
      }
      buffer = new ArrayBuffer[Tuple]()
    }
  }

}
