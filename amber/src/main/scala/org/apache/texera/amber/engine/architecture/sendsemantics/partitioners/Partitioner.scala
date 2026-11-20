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
    // Columnar wire: flush as VectorFrame (Arrow IPC) instead of DataFrame. The
    // planner decides this per edge (it folds in ApplicationConfig.enableColumnarWire
    // and the downstream operator's columnar capability) and passes it in; off by
    // default so the row wire is unchanged. A var so the output manager can set it.
    var columnar: Boolean = false,
    // Allocator for building a live Arrow root on a local columnar handoff. Null disables
    // the local columnar path (the edge falls back to rows).
    val columnarAllocator: BufferAllocator = null
) {

  // Set by the output manager: whether `to` is co-located (same ActorSystem). A local hop
  // never serializes, so the columnar wire is pure overhead there; None means not yet
  // known, treated as remote.
  var localityCheck: () => Option[Boolean] = () => None

  // The columnar decision after locality: honor the planner's flag only for a receiver
  // that is remote (or not yet known). A known-local receiver stays on rows.
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

  // Columnar fast path: ship an already-serialized Arrow batch straight to the
  // receiver, skipping the per-tuple accumulate/re-encode. Any tuples already
  // buffered are flushed first so the batch keeps its place in the FIFO stream.
  def sendVectorFrame(frame: VectorFrame): Unit = {
    flush()
    dataOutputPort.sendTo(to, frame)
  }

  // Row counterpart of sendVectorFrame: ship a prebuilt row batch (a DataFrame) to a
  // local receiver, flushing any buffered tuples first to keep FIFO order. Used when a
  // mixed-locality edge sends Arrow to its remote receivers and rows to its local ones.
  def sendRows(tuples: Array[Tuple]): Unit = {
    flush()
    if (tuples.nonEmpty) dataOutputPort.sendTo(to, DataFrame(tuples))
  }

  // Local zero-copy: hand the receiver a live Arrow root by reference (no serialization).
  // Ownership transfers to the receiver; the sender must not touch the root after this.
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
        // Local columnar: build one live Arrow root and hand it to the co-located
        // columnar-capable receiver by reference (no serialization), so it processes
        // columns in a batch instead of per-tuple rows. Ownership transfers to the receiver.
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
