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
import org.apache.texera.amber.core.executor.{
  ColumnarOperatorExecutor,
  ColumnarResult,
  OperatorExecutor
}
import org.apache.texera.amber.core.tuple.{Schema, Tuple, TupleLike}
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

  // ---- Native-Arrow path: consume the Arrow batch by decoding only the group
  // keys and aggregated columns per row, then feeding the same processTuple
  // accumulation. Blocking operator, so it emits nothing per batch (Consumed);
  // results are produced at onFinish via the row path.
  @transient private var columnarAllocator: RootAllocator = _
  @transient private var neededNames: Seq[String] = _
  @transient private var projSchema: Schema = _
  @transient private var projIndices: Array[Int] = _

  override def processColumnarBatch(arrowIpcBytes: Array[Byte]): ColumnarResult = {
    if (neededNames == null) {
      neededNames = (desc.groupByKeys ++ desc.aggregations.flatMap(a =>
        Option(a.attribute).map(_.trim).filter(_.nonEmpty)
      )).distinct
    }
    if (columnarAllocator == null) columnarAllocator = new RootAllocator()
    ArrowUtils.deserializeRootFold(arrowIpcBytes, columnarAllocator) { root =>
      if (projSchema == null) {
        val full = ArrowUtils.toTexeraSchema(root.getSchema)
        projSchema = Schema(neededNames.map(full.getAttribute).toList)
        projIndices = ArrowUtils.projectionIndices(root, neededNames)
      }
      val n = root.getRowCount
      var i = 0
      while (i < n) {
        processTuple(ArrowUtils.getProjectedTuple(i, root, projSchema, projIndices), 0)
        i += 1
      }
      ColumnarResult.Consumed
    }
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
