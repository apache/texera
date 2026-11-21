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
import org.apache.texera.amber.core.tuple.{Schema, SchemaEnforceable, Tuple, TupleLike}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.ArrowUtils

import scala.collection.mutable.ArrayBuffer

object SourceOperatorExecutor {
  // Rows per Arrow batch on the generic columnar path: large enough to amortize vectorized
  // downstream processing, small enough to stream and bound a pause to one batch.
  val ColumnarBatchRows = 10000
}

trait SourceOperatorExecutor extends OperatorExecutor {
  override def open(): Unit = {}

  override def close(): Unit = {}
  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = Iterator()

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty

  def produceTuple(): Iterator[TupleLike]

  /**
    * Columnar output for this source, honored only when the run is in columnar mode and the
    * downstream edge consumes columns. Every source is columnar-capable: the default batches
    * produceTuple's rows into Arrow VectorSchemaRoots (ColumnarBatchRows per batch) matching the
    * given output-port schema. It builds a Tuple only to copy its fields into Arrow vectors and
    * never reads Tuple.inMemSize, so with the lazy size it pays no deepSizeOf walk. A source whose
    * data is already columnar (Arrow) or decodes columns natively (Parquet) or parses straight
    * into vectors (CSV) overrides this to skip the Tuple entirely. The caller owns and closes each
    * returned root; roots are allocated in ArrowUtils.sharedAllocator so a handed-off batch
    * outlives this executor.
    */
  def produceColumnarBatch(schema: Schema): Option[Iterator[VectorSchemaRoot]] = {
    val rows = produceTuple()
    Some(new Iterator[VectorSchemaRoot] {
      override def hasNext: Boolean = rows.hasNext
      override def next(): VectorSchemaRoot = {
        val batch = new ArrayBuffer[Tuple](SourceOperatorExecutor.ColumnarBatchRows)
        while (batch.size < SourceOperatorExecutor.ColumnarBatchRows && rows.hasNext) {
          rows.next() match {
            case se: SchemaEnforceable => batch += se.enforceSchema(schema)
            case other                 => batch += TupleLike(other.getFields).enforceSchema(schema)
          }
        }
        ArrowUtils.rootFromTuples(schema, batch.toArray, ArrowUtils.sharedAllocator)
      }
    })
  }

  override def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] = {
    // We assume there is only one input port for source operators. The current assumption
    // makes produceTuple to be invoked on each input port finish.
    // We should move this to onFinishAllPorts later.
    produceTuple().map(t => (t, Option.empty))
  }
}
