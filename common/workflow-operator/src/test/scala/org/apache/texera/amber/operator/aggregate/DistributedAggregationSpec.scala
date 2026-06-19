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
import org.scalatest.matchers.should.Matchers

class DistributedAggregationSpec extends AnyFlatSpec with Matchers {

  // Representative average aggregation: partial = (sum, count)
  private val avg: DistributedAggregation[(Double, Long)] = DistributedAggregation(
    init    = () => (0.0, 0L),
    iterate = (p, t) => (p._1 + t.getField[Double]("value"), p._2 + 1L),
    merge   = (a, b) => (a._1 + b._1, a._2 + b._2),
    finalAgg = p => (p._1 / p._2).asInstanceOf[Object]
  )

  private val schema: Schema =
    Schema(List(new Attribute("value", AttributeType.DOUBLE)))

  private def tuple(v: Double): Tuple = Tuple(schema, Array(v))

  // -------------------------------------------------------------------------
  // init
  // -------------------------------------------------------------------------

  "DistributedAggregation.init" should "return the identity partial (0.0, 0L)" in {
    avg.init() shouldBe (0.0, 0L)
  }

  // -------------------------------------------------------------------------
  // iterate
  // -------------------------------------------------------------------------

  "DistributedAggregation.iterate" should "accumulate sum and count from each tuple" in {
    var p = avg.init()
    p = avg.iterate(p, tuple(3.0))
    p = avg.iterate(p, tuple(7.0))
    p shouldBe (10.0, 2L)
  }

  it should "leave the partial unchanged when called on an empty sequence" in {
    val p = avg.init()
    // iterating zero times is the empty-partition case
    p shouldBe avg.init()
  }

  // -------------------------------------------------------------------------
  // merge
  // -------------------------------------------------------------------------

  "DistributedAggregation.merge" should "add two partials' sums and counts together" in {
    val a = (10.0, 2L)
    val b = (15.0, 3L)
    avg.merge(a, b) shouldBe (25.0, 5L)
  }

  it should "be commutative: merge(a, b) == merge(b, a)" in {
    val a = (10.0, 4L)
    val b = (6.0, 2L)
    avg.merge(a, b) shouldBe avg.merge(b, a)
  }

  it should "be associative: merge(merge(a,b),c) == merge(a,merge(b,c))" in {
    val a = (2.0, 1L)
    val b = (4.0, 2L)
    val c = (6.0, 3L)
    avg.merge(avg.merge(a, b), c) shouldBe avg.merge(a, avg.merge(b, c))
  }

  it should "return the other partial unchanged when one side is init()" in {
    val p = (30.0, 6L)
    avg.merge(avg.init(), p) shouldBe p
    avg.merge(p, avg.init()) shouldBe p
  }

  // -------------------------------------------------------------------------
  // finalAgg
  // -------------------------------------------------------------------------

  "DistributedAggregation.finalAgg" should "compute sum / count" in {
    val result = avg.finalAgg((30.0, 6L)).asInstanceOf[Double]
    result shouldBe 5.0 +- 1e-9
  }

  it should "equal the naive mean of the iterated values" in {
    val values = Seq(1.0, 2.0, 3.0, 4.0, 5.0)
    var p = avg.init()
    values.foreach(v => p = avg.iterate(p, tuple(v)))
    val result = avg.finalAgg(p).asInstanceOf[Double]
    result shouldBe 3.0 +- 1e-9
  }

  // -------------------------------------------------------------------------
  // distributed == single-node
  // -------------------------------------------------------------------------

  "DistributedAggregation distributed execution" should
    "yield the same result as a single-node fold when input is split across two partitions" in {
    // All tuples processed on one node
    val all = Seq(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)
    var singleP = avg.init()
    all.foreach(v => singleP = avg.iterate(singleP, tuple(v)))
    val singleResult = avg.finalAgg(singleP).asInstanceOf[Double]

    // Same tuples split across two partitions
    val part1 = Seq(1.0, 2.0, 3.0)
    val part2 = Seq(4.0, 5.0, 6.0)

    var p1 = avg.init()
    part1.foreach(v => p1 = avg.iterate(p1, tuple(v)))

    var p2 = avg.init()
    part2.foreach(v => p2 = avg.iterate(p2, tuple(v)))

    val merged = avg.merge(p1, p2)
    val distResult = avg.finalAgg(merged).asInstanceOf[Double]

    distResult shouldBe singleResult +- 1e-9
  }

  it should "still equal a single-node fold when one partition is empty" in {
    val values = Seq(10.0, 20.0, 30.0)
    var singleP = avg.init()
    values.foreach(v => singleP = avg.iterate(singleP, tuple(v)))
    val singleResult = avg.finalAgg(singleP).asInstanceOf[Double]

    // partition A has all the data; partition B contributes nothing (init)
    var pA = avg.init()
    values.foreach(v => pA = avg.iterate(pA, tuple(v)))
    val pB = avg.init()

    val merged = avg.merge(pA, pB)
    val distResult = avg.finalAgg(merged).asInstanceOf[Double]

    distResult shouldBe singleResult +- 1e-9
  }
}
