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

package org.apache.texera.amber.engine.common.ambermessage

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Schema, Tuple}

sealed trait DataPayload extends WorkflowFIFOMessagePayload {}

/**
  * A single State travelling between operators. `loopCounter` / `loopStartId`
  * are the loop envelope owned by the (Python) worker runtime -- carried
  * alongside the State payload (not inside it) so it never collides with user
  * state, and materialized/transported as their own columns parallel to the
  * content (see `State.toTuple`). Loop operators are Python-only, so a JVM
  * operator inside a loop body only ever carries the envelope through
  * unchanged; the defaults are the "no loop" values for all non-loop state.
  * Mirrors the Python `StateFrame` (core/models/payload.py).
  */
final case class StateFrame(frame: State, loopCounter: Long = 0L, loopStartId: String = "")
    extends DataPayload

/**
  * A batch of rows carried over the wire as Arrow IPC stream bytes (the columnar
  * wire format), instead of as decoded `Tuple`s in a `DataFrame`. `rowCount` and
  * the Texera `schema` describe the batch without decoding it. The receiver either
  * consumes the bytes directly (a `ColumnarOperatorExecutor`) or decodes them back
  * to tuples (`ArrowUtils.deserializeTuples`). Produced only when the columnar wire
  * is enabled; off by default, so the default wire stays `DataFrame`.
  */
final case class VectorFrame(arrowIpcBytes: Array[Byte], rowCount: Int, schema: Schema)
    extends DataPayload {
  val inMemSize: Long = arrowIpcBytes.length.toLong
}

/**
  * A batch handed to a co-located (same-JVM) receiver as a LIVE Arrow root, by reference,
  * with no serialization at all: the local zero-copy wire. Ownership transfers to the
  * receiver, which closes `root` after consuming it (via processColumnarRoot); the sender
  * must not touch it after sending. Produced only for a single-consumer local columnar
  * edge; a remote hop uses `VectorFrame` and a non-columnar receiver uses `DataFrame`.
  * A custom Kryo serializer (AmberKryoInitializer) converts it to/from Arrow IPC bytes so
  * an accidental serialization (e.g. a checkpoint of an in-flight batch) stays correct,
  * losing only the zero-copy property.
  */
final case class LocalColumnarBatch(root: VectorSchemaRoot, rowCount: Int, schema: Schema)
    extends DataPayload {
  val inMemSize: Long = {
    val vecs = root.getFieldVectors
    var sum = 0L
    var i = 0
    while (i < vecs.size) { sum += vecs.get(i).getBufferSize.toLong; i += 1 }
    sum
  }
}

final case class DataFrame(frame: Array[Tuple]) extends DataPayload {
  val inMemSize: Long = {
    frame.map(_.inMemSize).sum
  }

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[DataFrame]) return false
    val other = obj.asInstanceOf[DataFrame]
    if (other eq null) return false
    if (frame.length != other.frame.length) {
      return false
    }
    var i = 0
    while (i < frame.length) {
      if (frame(i) != other.frame(i)) {
        return false
      }
      i += 1
    }
    true
  }
}
