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

package org.apache.texera.amber.operator.aggregate

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.texera.amber.core.executor.{
  ColumnarOperatorExecutor,
  ColumnarResult,
  OperatorExecutor
}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.mutable

/**
  * AggregateOpExec performs aggregation operations on input tuples, optionally grouping them by specified keys.
  */
class AggregateOpExec(descString: String) extends OperatorExecutor with ColumnarOperatorExecutor {
  private val desc: AggregateOpDesc = objectMapper.readValue(descString, classOf[AggregateOpDesc])
  private var keyedPartialAggregates: mutable.HashMap[List[Object], List[Object]] = _
  private var distributedAggregations: List[DistributedAggregation[Object]] = _

  override def open(): Unit = {
    keyedPartialAggregates = new mutable.HashMap[List[Object], List[Object]]()
    distributedAggregations = null
  }

  override def close(): Unit = {
    keyedPartialAggregates.clear()
    distributedAggregations = null
    if (columnarAllocator != null) { columnarAllocator.close(); columnarAllocator = null }
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    // Initialize distributedAggregations if it's not yet initialized
    if (distributedAggregations == null) {
      distributedAggregations = desc.aggregations.map { agg =>
        // Only COUNT with an empty attribute (COUNT(*)) skips the column lookup; its
        // result does not depend on any input attribute. Every other function resolves
        // the input attribute (failing fast if it is missing/invalid).
        val attrType =
          if (
            agg.aggFunction == AggregationFunction.COUNT &&
            (agg.attribute == null || agg.attribute.trim.isEmpty)
          ) null
          else tuple.getSchema.getAttribute(agg.attribute).getType
        agg.getAggFunc(attrType)
      }
    }

    // Construct the group key
    val key = desc.groupByKeys.map(tuple.getField[Object])

    // Get or initialize the partial aggregate for the key
    val partialAggregates =
      keyedPartialAggregates.getOrElseUpdate(key, distributedAggregations.map(_.init()))

    // Update the partial aggregates with the current tuple
    val updatedAggregates = (distributedAggregations zip partialAggregates).map {
      case (aggregation, partial) => aggregation.iterate(partial, tuple)
    }

    keyedPartialAggregates(key) = updatedAggregates
    Iterator.empty

  }

  // ---- Native-Arrow path: accumulate an Arrow batch by reading only the group
  // keys and aggregated cells per row (no projected Tuple), feeding the same
  // per-value accumulation (iterateValue) the row path uses. Blocking operator, so
  // it emits nothing per batch (Consumed); results are produced at onFinish.
  @transient private var columnarAllocator: RootAllocator = _
  // Per-batch resolution: the source-column index and Texera type of each group-by
  // key, and of each aggregation's input column (index -1 for COUNT(*), which reads
  // no column).
  @transient private var colGroupIndices: Array[Int] = _
  @transient private var colGroupTypes: Array[AttributeType] = _
  @transient private var colAggIndices: Array[Int] = _
  @transient private var colAggTypes: Array[AttributeType] = _

  private def aggInputType(agg: AggregationOperation, full: Schema): AttributeType =
    if (
      agg.aggFunction == AggregationFunction.COUNT &&
      (agg.attribute == null || agg.attribute.trim.isEmpty)
    ) null
    else full.getAttribute(agg.attribute).getType

  // Accumulate one Arrow batch into the group state, reading each group key and
  // aggregated value straight from its Arrow vector. Mirrors processTuple's
  // accumulation exactly, feeding each aggregation the already-extracted value via
  // iterateValue. Blocking operator, so it emits nothing now (Consumed). Does not
  // close `root`.
  private def accumulateRoot(root: VectorSchemaRoot): ColumnarResult = {
    if (distributedAggregations == null || colGroupIndices == null) {
      val full = ArrowUtils.toTexeraSchema(root.getSchema)
      if (distributedAggregations == null) {
        distributedAggregations =
          desc.aggregations.map(agg => agg.getAggFunc(aggInputType(agg, full)))
      }
      colGroupIndices = ArrowUtils.projectionIndices(root, desc.groupByKeys)
      colGroupTypes = desc.groupByKeys.map(full.getAttribute(_).getType).toArray
      colAggIndices = desc.aggregations.map { agg =>
        if (agg.attribute == null || agg.attribute.trim.isEmpty) -1
        else ArrowUtils.projectionIndices(root, Seq(agg.attribute))(0)
      }.toArray
      colAggTypes = desc.aggregations.map(agg => aggInputType(agg, full)).toArray
    }
    val vectors = root.getFieldVectors
    val n = root.getRowCount
    val numGroups = colGroupIndices.length
    var i = 0
    while (i < n) {
      val key =
        (0 until numGroups)
          .map(g => ArrowUtils.getCell(vectors.get(colGroupIndices(g)), i, colGroupTypes(g)))
          .toList
      val partials =
        keyedPartialAggregates.getOrElseUpdate(key, distributedAggregations.map(_.init()))
      val updated = distributedAggregations.zip(partials).zipWithIndex.map {
        case ((aggregation, partial), j) =>
          val idx = colAggIndices(j)
          val value: Object =
            if (idx < 0) null else ArrowUtils.getCell(vectors.get(idx), i, colAggTypes(j))
          aggregation.iterateValue(partial, value)
      }
      keyedPartialAggregates(key) = updated
      i += 1
    }
    ColumnarResult.Consumed
  }

  override def processColumnarBatch(arrowIpcBytes: Array[Byte], port: Int): ColumnarResult = {
    if (columnarAllocator == null) columnarAllocator = new RootAllocator()
    ArrowUtils.deserializeRootFold(arrowIpcBytes, columnarAllocator)(accumulateRoot)
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    // Finalize aggregation for all keys and produce the result
    keyedPartialAggregates.iterator.map {
      case (key, partials) =>
        val finalAggregates = partials.zipWithIndex.map {
          case (partial, index) => distributedAggregations(index).finalAgg(partial)
        }
        TupleLike(key ++ finalAggregates)
    }
  }
}
