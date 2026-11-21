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

package org.apache.texera.amber.operator.filter

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{Float8Vector, IntVector, VectorSchemaRoot}
import org.apache.texera.amber.core.executor.{ColumnarOperatorExecutor, ColumnarResult}
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper

class SpecializedFilterOpExec(descString: String)
    extends FilterOpExec
    with ColumnarOperatorExecutor {
  private val desc: SpecializedFilterOpDesc =
    objectMapper.readValue(descString, classOf[SpecializedFilterOpDesc])
  // Row path: OR over predicates, per tuple.
  setFilterFunc((tuple: Tuple) => desc.predicates.exists(_.evaluate(tuple)))

  private def cmpOf(c: ComparisonType): Int => Boolean =
    c match {
      case ComparisonType.GREATER_THAN             => (r: Int) => r > 0
      case ComparisonType.GREATER_THAN_OR_EQUAL_TO => (r: Int) => r >= 0
      case ComparisonType.LESS_THAN                => (r: Int) => r < 0
      case ComparisonType.LESS_THAN_OR_EQUAL_TO    => (r: Int) => r <= 0
      case ComparisonType.EQUAL_TO                 => (r: Int) => r == 0
      case ComparisonType.NOT_EQUAL_TO             => (r: Int) => r != 0
      case _                                       => null
    }

  // Native-Arrow path: consume the Arrow batch directly, no per-tuple decode.
  @transient private var columnarAllocator: RootAllocator = _

  // Keep mask for a single supported numeric predicate over the batch, or None if
  // the predicate/column shape is not vectorizable (caller falls back to the row
  // path). Shared by the emit path and the selection-vector path.
  private def columnarMask(root: VectorSchemaRoot): Option[Array[Boolean]] = {
    if (desc.predicates.size != 1) return None
    val p = desc.predicates.head
    val cmp = cmpOf(p.condition)
    if (cmp == null) return None
    val c =
      try p.value.toDouble
      catch { case _: NumberFormatException => return None }
    val fields = root.getSchema.getFields
    var fieldIdx = -1
    var k = 0
    while (k < fields.size && fieldIdx < 0) {
      if (fields.get(k).getName == p.attribute) fieldIdx = k
      k += 1
    }
    if (fieldIdx < 0) return None
    val n = root.getRowCount
    val mask = new Array[Boolean](n)
    root.getVector(fieldIdx) match {
      case v: Float8Vector =>
        var i = 0
        while (i < n) {
          mask(i) = !v.isNull(i) && cmp(java.lang.Double.compare(v.get(i), c)); i += 1
        }
        Some(mask)
      case v: IntVector =>
        var i = 0
        while (i < n) {
          mask(i) = !v.isNull(i) && cmp(java.lang.Double.compare(v.get(i).toDouble, c)); i += 1
        }
        Some(mask)
      case _ => None // non-numeric column: fall back to the row path
    }
  }

  override def processColumnarBatch(arrowIpcBytes: Array[Byte], port: Int): ColumnarResult = {
    if (columnarAllocator == null) columnarAllocator = new RootAllocator()
    ArrowUtils.deserializeRootFold(arrowIpcBytes, columnarAllocator) { root =>
      columnarMask(root) match {
        case Some(mask) => ColumnarResult.Emit(ArrowUtils.selectRows(root, mask, columnarAllocator))
        case None       => ColumnarResult.Unsupported
      }
    }
  }

  // Selection-vector path: the indices of surviving rows, computed without
  // compacting or copying the batch. None when the predicate is not vectorizable.
  // For a fused consumer (count/aggregate) that reads the indices instead of a
  // materialized batch.
  def columnarSelection(arrowIpcBytes: Array[Byte], port: Int): Option[Array[Int]] = {
    if (columnarAllocator == null) columnarAllocator = new RootAllocator()
    ArrowUtils.deserializeRootFold(arrowIpcBytes, columnarAllocator) { root =>
      columnarMask(root).map(mask => ArrowUtils.selectionOf(mask, root.getRowCount))
    }
  }

  // Fused filter -> count: number of surviving rows, no batch materialized.
  def columnarMatchCount(arrowIpcBytes: Array[Byte], port: Int): Option[Long] =
    columnarSelection(arrowIpcBytes, port).map(_.length.toLong)

  override def close(): Unit = {
    if (columnarAllocator != null) columnarAllocator.close()
  }
}
