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
}

class NetworkOutputBuffer(
    val to: ActorVirtualIdentity,
    val dataOutputPort: NetworkOutputGateway,
    val batchSize: Int = ApplicationConfig.defaultDataTransferBatchSize,
    // Columnar wire (opt-in): flush as VectorFrame (Arrow IPC) instead of
    // DataFrame. Off by default, so the default wire is unchanged. A var so the
    // output manager can set it per edge (phase 2's planner decides this per link).
    var columnar: Boolean = ApplicationConfig.enableColumnarWire
) {

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
      if (columnar) {
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
