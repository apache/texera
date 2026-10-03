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

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.mutable

/**
  * AggregateOpExec performs aggregation operations on input tuples, optionally grouping them by specified keys.
  */
class AggregateOpExec protected (
    descString: String,
    emitPartials: Boolean,
    mergePartials: Boolean
) extends OperatorExecutor {
  def this(descString: String) = this(descString, emitPartials = false, mergePartials = false)

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
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    // Initialize distributedAggregations if it's not yet initialized
    if (distributedAggregations == null) {
      distributedAggregations = desc.aggregations.map { agg =>
        // Only COUNT with an empty attribute (COUNT(*)) skips the column lookup; its
        // result does not depend on any input attribute. Every other function resolves
        // the input attribute (failing fast if it is missing/invalid).
        val attrType =
          if (mergePartials) tuple.getSchema.getAttribute(agg.resultAttribute).getType
          else if (
            agg.aggFunction == AggregationFunction.COUNT &&
            (agg.attribute == null || agg.attribute.trim.isEmpty)
          ) null
          else tuple.getSchema.getAttribute(agg.attribute).getType
        if (mergePartials) agg.getAggFunc(attrType)
        else agg.getAggFunc(attrType, tuple.getSchema)
      }
    }

    // Construct the group key
    val key = desc.groupByKeys.map(tuple.getField[Object])

    // Get or initialize the partial aggregate for the key
    val partialAggregates =
      keyedPartialAggregates.getOrElseUpdate(key, distributedAggregations.map(_.init()))

    // Update the partial aggregates with the current tuple
    val updatedAggregates = (distributedAggregations zip partialAggregates).zipWithIndex.map {
      case ((aggregation, partial), index) =>
        if (mergePartials) {
          val operation = desc.aggregations(index)
          val value = tuple.getField[Object](operation.resultAttribute)
          val incoming = operation.aggFunction match {
            case AggregationFunction.AVERAGE =>
              val buffer = ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
              AveragePartialObj(buffer.getDouble(), buffer.getDouble())
            case AggregationFunction.CONCAT =>
              val buffer = ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
              val leadingEmptyCount = buffer.getLong()
              val text = new Array[Byte](buffer.remaining())
              buffer.get(text)
              ConcatPartialObj(new String(text, UTF_8), leadingEmptyCount)
            case _ => value
          }
          aggregation.merge(partial, incoming)
        } else aggregation.iterate(partial, tuple)
    }

    keyedPartialAggregates(key) = updatedAggregates
    Iterator.empty

  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    // Finalize aggregation for all keys and produce the result
    keyedPartialAggregates.iterator.map {
      case (key, partials) =>
        val finalAggregates = partials.zipWithIndex.map {
          case (partial, index) =>
            if (!emitPartials) distributedAggregations(index).finalAgg(partial)
            else
              partial match {
                // Carry both sum and count over the typed transport; finalize only
                // after merging every worker, including workers with no matches.
                case avg: AveragePartialObj =>
                  ByteBuffer.allocate(16).putDouble(avg.sum).putDouble(avg.count).array()
                case concat: ConcatPartialObj =>
                  val text = concat.value.getBytes(UTF_8)
                  ByteBuffer
                    .allocate(8 + text.length)
                    .putLong(concat.leadingEmptyCount)
                    .put(text)
                    .array()
                case value => value
              }
        }
        TupleLike(key ++ finalAggregates)
    }
  }
}

class PartialAggregateOpExec(descString: String)
    extends AggregateOpExec(descString, emitPartials = true, mergePartials = false)

class FinalAggregateOpExec(descString: String)
    extends AggregateOpExec(descString, emitPartials = false, mergePartials = true)
