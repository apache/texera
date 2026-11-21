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
  // Rows per Arrow batch on the generic columnar path.
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
    * This source's output as Arrow batches (used in columnar mode). The default batches
    * produceTuple's rows into roots of ColumnarBatchRows matching `schema`; it builds Tuples only
    * to copy their fields in and never reads inMemSize, so it pays no deepSizeOf. Arrow/Parquet/CSV
    * override this to skip the Tuple. Roots come from ArrowUtils.sharedAllocator; the caller closes them.
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
