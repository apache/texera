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

package org.apache.texera.amber.core.executor

import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.texera.amber.core.tuple.TupleLike
import org.apache.texera.amber.core.workflow.PortIdentity

/**
  * Outcome of consuming one Arrow batch:
  *   - Emit: consumed, emit this Arrow result batch downstream (caller serializes and closes the root).
  *   - EmitRows: consumed, emit these row outputs (e.g. a join whose output is row-shaped);
  *     the caller ships them, still columnar on the wire if enabled.
  *   - Consumed: consumed, nothing to emit now (a blocking operator accumulating state).
  *   - Unsupported: not handled, caller falls back to the row path (decode + processTuple).
  */
sealed trait ColumnarResult
object ColumnarResult {
  final case class Emit(root: VectorSchemaRoot) extends ColumnarResult
  final case class EmitRows(rows: Iterator[(TupleLike, Option[PortIdentity])])
      extends ColumnarResult
  case object Consumed extends ColumnarResult
  case object Unsupported extends ColumnarResult
}

/**
  * An operator that can consume an Arrow columnar batch directly (no per-row
  * Tuple decode). Given the incoming batch as Arrow IPC bytes and the input
  * port it arrived on, returns a ColumnarResult telling the caller whether it
  * emitted a batch/rows, consumed the batch with no output, or could not handle
  * it (fall back to the row path).
  */
trait ColumnarOperatorExecutor {
  def processColumnarBatch(arrowIpcBytes: Array[Byte], port: Int): ColumnarResult
}
