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
import org.apache.texera.amber.engine.common.ambermessage.{VectorFrame, DataFrame, StateFrame}
import org.apache.texera.amber.util.ArrowUtils

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
    var columnar: Boolean = false
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
      } else {
        dataOutputPort.sendTo(to, DataFrame(batch))
      }
      buffer = new ArrayBuffer[Tuple]()
    }
  }

}
