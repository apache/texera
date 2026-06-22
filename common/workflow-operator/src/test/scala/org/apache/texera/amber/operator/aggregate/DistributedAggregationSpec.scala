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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.scalatest.flatspec.AnyFlatSpec

class DistributedAggregationSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixtures — a Tuple-shaped average example
  // ---------------------------------------------------------------------------

  private val attr = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(attr)
  private def tup(value: Int): Tuple =
    Tuple.builder(schema).add(attr, Integer.valueOf(value)).build()

  // Partial = (sum, count)
  private val avgAggregation = DistributedAggregation[(Long, Long)](
    init = () => (0L, 0L),
    iterate = (partial, t) => (partial._1 + t.getField[Integer]("v").longValue(), partial._2 + 1L),
    merge = (a, b) => (a._1 + b._1, a._2 + b._2),
    finalAgg = partial =>
      if (partial._2 == 0L) java.lang.Double.valueOf(0.0d)
      else java.lang.Double.valueOf(partial._1.toDouble / partial._2.toDouble)
  )

  // ---------------------------------------------------------------------------
  // Case-class shape
  // ---------------------------------------------------------------------------

  "DistributedAggregation" should "expose all four function members" in {
    assert(avgAggregation.init != null)
    assert(avgAggregation.iterate != null)
    assert(avgAggregation.merge != null)
    assert(avgAggregation.finalAgg != null)
  }

  it should "be a case class (equality on same function refs)" in {
    val sameInit = avgAggregation.init
    val sameIter = avgAggregation.iterate
    val sameMerge = avgAggregation.merge
    val sameFinal = avgAggregation.finalAgg
    val copy = DistributedAggregation[(Long, Long)](sameInit, sameIter, sameMerge, sameFinal)
    assert(copy == avgAggregation)
  }

  // ---------------------------------------------------------------------------
  // init — produces a fresh partial each call
  // ---------------------------------------------------------------------------

  "init" should "produce the zero partial" in {
    assert(avgAggregation.init() == (0L, 0L))
  }

  // ---------------------------------------------------------------------------
  // iterate — folds one input tuple into the partial
  // ---------------------------------------------------------------------------

  "iterate" should "fold one tuple into the partial (sum += value, count += 1)" in {
    val p0 = avgAggregation.init()
    val p1 = avgAggregation.iterate(p0, tup(7))
    assert(p1 == (7L, 1L))
    val p2 = avgAggregation.iterate(p1, tup(13))
    assert(p2 == (20L, 2L))
  }

  // ---------------------------------------------------------------------------
  // merge — adds two partials componentwise
  // ---------------------------------------------------------------------------

  "merge" should "add two partials componentwise" in {
    val merged = avgAggregation.merge((10L, 3L), (5L, 2L))
    assert(merged == (15L, 5L))
  }

  // ---------------------------------------------------------------------------
  // finalAgg — divides sum by count
  // ---------------------------------------------------------------------------

  "finalAgg" should "divide sum by count" in {
    val result = avgAggregation.finalAgg((15L, 5L)).asInstanceOf[java.lang.Double]
    assert(result.doubleValue() == 3.0d)
  }

  it should "guard against division by zero on the zero partial" in {
    val result = avgAggregation.finalAgg((0L, 0L)).asInstanceOf[java.lang.Double]
    assert(result.doubleValue() == 0.0d)
  }

  // ---------------------------------------------------------------------------
  // End-to-end — single-node fold reproduces the textbook average
  // ---------------------------------------------------------------------------

  "DistributedAggregation (end-to-end)" should "compute the average of [1..5] = 3.0" in {
    val inputs = (1 to 5).map(tup)
    val partial = inputs.foldLeft(avgAggregation.init())(avgAggregation.iterate)
    val finalResult = avgAggregation.finalAgg(partial).asInstanceOf[java.lang.Double]
    assert(finalResult.doubleValue() == 3.0d)
  }

  it should "compute the same average via two partial nodes + merge" in {
    val left = Seq(1, 2, 3).map(tup).foldLeft(avgAggregation.init())(avgAggregation.iterate)
    val right = Seq(4, 5).map(tup).foldLeft(avgAggregation.init())(avgAggregation.iterate)
    val merged = avgAggregation.merge(left, right)
    val finalResult = avgAggregation.finalAgg(merged).asInstanceOf[java.lang.Double]
    assert(finalResult.doubleValue() == 3.0d)
  }
}
