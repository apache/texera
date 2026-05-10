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

/**
  * Coverage notes:
  * `AggregateOpSpec` (in this same package) already exercises the happy paths for
  * `getAggregationAttribute`, the per-kind `init` / `iterate` / `finalAgg` semantics
  * (SUM/COUNT/AVERAGE/MIN/MAX/CONCAT, including null-handling and AVERAGE-of-empty),
  * and the `getFinal` rewrite. This spec deliberately does NOT duplicate those.
  *
  * What this spec adds:
  * - `getAggFunc` validation errors (unsupported attribute types on SUM/MIN/MAX,
  *   null aggFunction). Note: SUM/MIN/MAX do accept TIMESTAMP alongside numerics.
  * - The CONCAT-specific `merge` partial-combination behavior — `AggregateOpSpec`
  *   exercises iterate/finalAgg but never calls `merge` directly.
  * - A two-stage worker→final pipeline that runs a real partial aggregation
  *   on each "worker", emits a partial tuple, then applies `getFinal` and
  *   re-aggregates the partials end-to-end.
  * - `AveragePartialObj` (a plain `case class`, not a value class) field
  *   exposure and case-class value equality / hashCode.
  */
class AggregationOperationSpec extends AnyFlatSpec {

  // --- helpers ---------------------------------------------------------------

  private def schemaWith(name: String, t: AttributeType): Schema =
    new Schema(new Attribute(name, t))

  private def tupleOf(name: String, t: AttributeType, value: AnyRef): Tuple =
    Tuple.builder(schemaWith(name, t)).add(new Attribute(name, t), value).build()

  private def op(
      func: AggregationFunction,
      attribute: String = "v",
      resultAttribute: String = "r"
  ): AggregationOperation = {
    val o = new AggregationOperation()
    o.aggFunction = func
    o.attribute = attribute
    o.resultAttribute = resultAttribute
    o
  }

  // --- getAggFunc: type validation (not covered in AggregateOpSpec) ----------

  "AggregationOperation.getAggFunc" should "throw UnsupportedOperationException for unsupported attribute types on SUM" in {
    // SUM accepts INTEGER/LONG/DOUBLE/TIMESTAMP; STRING is rejected.
    val ex = intercept[UnsupportedOperationException] {
      op(AggregationFunction.SUM).getAggFunc(AttributeType.STRING)
    }
    assert(ex.getMessage.contains("Unsupported attribute type for sum"))
  }

  it should "throw UnsupportedOperationException for unsupported attribute types on MIN and MAX" in {
    // MIN/MAX accept INTEGER/LONG/DOUBLE/TIMESTAMP; STRING and BOOLEAN are rejected.
    intercept[UnsupportedOperationException] {
      op(AggregationFunction.MIN).getAggFunc(AttributeType.STRING)
    }
    intercept[UnsupportedOperationException] {
      op(AggregationFunction.MAX).getAggFunc(AttributeType.BOOLEAN)
    }
  }

  it should "throw UnsupportedOperationException when aggFunction is null" in {
    val ex = intercept[UnsupportedOperationException] {
      op(null).getAggFunc(AttributeType.INTEGER)
    }
    assert(ex.getMessage.contains("Unknown aggregation function"))
  }

  // --- CONCAT partial merge (iterate is covered in AggregateOpSpec) ----------

  "CONCAT aggregation merge" should
    "join two non-empty partials with a comma and short-circuit when either is empty" in {
    val agg = op(AggregationFunction.CONCAT).getAggFunc(AttributeType.STRING)
    assert(agg.merge("foo", "bar") == "foo,bar")
    assert(agg.merge("", "bar") == "bar")
    assert(agg.merge("foo", "") == "foo")
    assert(agg.merge("", "") == "")
  }

  // --- partial + final pipeline ----------------------------------------------

  "Worker → final aggregation pipeline" should
    "give the same total as a single-pass COUNT when partials are re-aggregated via getFinal" in {
    // Two "workers" each run a COUNT over their slice of the data. Each
    // worker emits a partial output (an Integer count). The "final" stage
    // re-aggregates those partial outputs as a SUM over the result column,
    // which getFinal is supposed to produce.
    val workerOp = op(AggregationFunction.COUNT, attribute = "v", resultAttribute = "row_count")
    val workerAgg = workerOp.getAggFunc(AttributeType.INTEGER)

    val w1Tuples = Seq(
      tupleOf("v", AttributeType.INTEGER, Int.box(10)),
      tupleOf("v", AttributeType.INTEGER, null),
      tupleOf("v", AttributeType.INTEGER, Int.box(20))
    )
    val w1State = w1Tuples.foldLeft(workerAgg.init())(workerAgg.iterate)
    val w1Out = workerAgg.finalAgg(w1State).asInstanceOf[Integer]
    assert(w1Out == 2, "worker 1 saw two non-null values")

    val w2Tuples = Seq(
      tupleOf("v", AttributeType.INTEGER, Int.box(30)),
      tupleOf("v", AttributeType.INTEGER, Int.box(40)),
      tupleOf("v", AttributeType.INTEGER, Int.box(50))
    )
    val w2State = w2Tuples.foldLeft(workerAgg.init())(workerAgg.iterate)
    val w2Out = workerAgg.finalAgg(w2State).asInstanceOf[Integer]
    assert(w2Out == 3)

    // Final stage: re-aggregate the partial counts via getFinal.
    val finalOp = workerOp.getFinal
    assert(finalOp.aggFunction == AggregationFunction.SUM)
    assert(finalOp.attribute == "row_count")
    val finalAgg = finalOp.getAggFunc(AttributeType.INTEGER)
    val partial1 = tupleOf("row_count", AttributeType.INTEGER, w1Out)
    val partial2 = tupleOf("row_count", AttributeType.INTEGER, w2Out)
    val finalState =
      finalAgg.iterate(finalAgg.iterate(finalAgg.init(), partial1), partial2)
    val finalCount = finalAgg.finalAgg(finalState).asInstanceOf[Integer]
    assert(finalCount == 5, "summing partial counts must match a single-pass COUNT")
  }

  it should
    "give the same total as a single-pass SUM when partials are re-aggregated via getFinal" in {
    // For SUM, getFinal keeps aggFunction = SUM and rebinds attribute to the
    // result column. The pipeline must produce the same total as a single-pass
    // SUM over all the input tuples.
    val workerOp = op(AggregationFunction.SUM, attribute = "v", resultAttribute = "total")
    val workerAgg = workerOp.getAggFunc(AttributeType.INTEGER)

    val groups = Seq(
      Seq(Int.box(1), Int.box(2), Int.box(3)),
      Seq(Int.box(10), Int.box(20))
    )
    val partials: Seq[Integer] = groups.map { values =>
      val state = values
        .map(v => tupleOf("v", AttributeType.INTEGER, v))
        .foldLeft(workerAgg.init())(workerAgg.iterate)
      workerAgg.finalAgg(state).asInstanceOf[Integer]
    }
    assert(partials == Seq(6: Integer, 30: Integer))

    val finalOp = workerOp.getFinal
    assert(finalOp.aggFunction == AggregationFunction.SUM)
    assert(finalOp.attribute == "total")
    val finalAgg = finalOp.getAggFunc(AttributeType.INTEGER)
    val finalState = partials
      .map(p => tupleOf("total", AttributeType.INTEGER, p))
      .foldLeft(finalAgg.init())(finalAgg.iterate)
    val finalSum = finalAgg.finalAgg(finalState).asInstanceOf[Integer]
    assert(finalSum == 36, "single-pass SUM(1+2+3+10+20) == 36")
  }

  // --- AveragePartialObj -----------------------------------------------------

  "AveragePartialObj" should "expose its sum and count fields and support value equality" in {
    val a = AveragePartialObj(10.0, 4)
    val b = AveragePartialObj(10.0, 4)
    assert(a.sum == 10.0)
    assert(a.count == 4)
    assert(a == b)
    assert(a.hashCode == b.hashCode)
  }
}
