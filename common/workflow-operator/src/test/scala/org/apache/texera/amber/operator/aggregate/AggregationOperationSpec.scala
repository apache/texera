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

import java.sql.Timestamp

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

  it should
    "keep the type's maximum value when partial MAX results are re-aggregated via getFinal" in {
    // Regression: the local stage used to finalize a partial equal to
    // maxValue(attributeType) to null, so the final stage reported the largest
    // value from the other worker instead of the true maximum.
    val workerOp = op(AggregationFunction.MAX, attribute = "v", resultAttribute = "max_v")
    val workerAgg = workerOp.getAggFunc(AttributeType.INTEGER)

    val w1State = Seq[AnyRef](Int.box(1), Int.box(Int.MaxValue))
      .map(v => tupleOf("v", AttributeType.INTEGER, v))
      .foldLeft(workerAgg.init())(workerAgg.iterate)
    val w1Out = workerAgg.finalAgg(w1State)
    assert(w1Out == Int.box(Int.MaxValue), "worker 1's true maximum is Int.MaxValue")

    val w2State = Seq[AnyRef](Int.box(5), Int.box(2))
      .map(v => tupleOf("v", AttributeType.INTEGER, v))
      .foldLeft(workerAgg.init())(workerAgg.iterate)
    val w2Out = workerAgg.finalAgg(w2State)
    assert(w2Out == Int.box(5))

    val finalOp = workerOp.getFinal
    assert(finalOp.aggFunction == AggregationFunction.MAX)
    val finalAgg = finalOp.getAggFunc(AttributeType.INTEGER)
    val finalState = Seq(w1Out, w2Out)
      .map(p => tupleOf("max_v", AttributeType.INTEGER, p))
      .foldLeft(finalAgg.init())(finalAgg.iterate)
    assert(finalAgg.finalAgg(finalState) == Int.box(Int.MaxValue))
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

  // --- getAggFunc: the accepting side of the four-clause type guard ----------

  // SUM/MIN/MAX each guard on
  // `!= INTEGER && != DOUBLE && != LONG && != TIMESTAMP`. The existing tests
  // only drive the rejecting side plus INTEGER/DOUBLE, so the chain is never
  // walked to its later clauses; TIMESTAMP in particular has to pass all four.
  private val supportedAggTypes = Seq(
    AttributeType.INTEGER,
    AttributeType.DOUBLE,
    AttributeType.LONG,
    AttributeType.TIMESTAMP
  )

  private val guardedAggregations = Seq(
    AggregationFunction.SUM -> "sum",
    AggregationFunction.MIN -> "min",
    AggregationFunction.MAX -> "max"
  )

  it should "accept every supported attribute type on SUM, MIN and MAX" in {
    for ((func, name) <- guardedAggregations; t <- supportedAggTypes)
      assert(op(func).getAggFunc(t) != null, s"$name should accept $t")
  }

  it should "reject unsupported attribute types on SUM, MIN and MAX, naming the aggregation and the type" in {
    val unsupported =
      Seq(AttributeType.STRING, AttributeType.BOOLEAN, AttributeType.BINARY)
    for ((func, name) <- guardedAggregations; t <- unsupported) {
      val ex = intercept[UnsupportedOperationException](op(func).getAggFunc(t))
      assert(ex.getMessage == s"Unsupported attribute type for $name aggregation: $t")
    }
  }

  // --- AVERAGE over TIMESTAMP: the timestamp branch of getNumericalValue -----

  // Everywhere else AVERAGE is driven over DOUBLE, which takes the
  // `value.toString.toDouble` path. A TIMESTAMP column is the only route into
  // the `parseTimestamp(...).getTime` branch.
  private val earlier = Timestamp.valueOf("2020-03-05 10:00:00")
  private val later = Timestamp.valueOf("2020-03-05 11:00:00")
  private val midpointMillis = (earlier.getTime + later.getTime) / 2.0

  "AVERAGE over a TIMESTAMP column" should "average the values' epoch milliseconds" in {
    val agg = op(AggregationFunction.AVERAGE).getAggFunc(AttributeType.TIMESTAMP)
    val state = Seq(earlier, later)
      .map(ts => tupleOf("v", AttributeType.TIMESTAMP, ts))
      .foldLeft(agg.init())(agg.iterate)

    assert(agg.finalAgg(state).asInstanceOf[java.lang.Double] == midpointMillis)
  }

  it should "combine per-worker partials through merge" in {
    val agg = op(AggregationFunction.AVERAGE).getAggFunc(AttributeType.TIMESTAMP)
    val p1 = agg.iterate(agg.init(), tupleOf("v", AttributeType.TIMESTAMP, earlier))
    val p2 = agg.iterate(agg.init(), tupleOf("v", AttributeType.TIMESTAMP, later))

    val merged = agg.merge(p1, p2)

    assert(agg.finalAgg(merged).asInstanceOf[java.lang.Double] == midpointMillis)
  }

  it should "ignore null timestamps and return null when every value is null" in {
    val agg = op(AggregationFunction.AVERAGE).getAggFunc(AttributeType.TIMESTAMP)

    val mixed = Seq[AnyRef](earlier, null, later)
      .map(ts => tupleOf("v", AttributeType.TIMESTAMP, ts))
      .foldLeft(agg.init())(agg.iterate)
    assert(agg.finalAgg(mixed).asInstanceOf[java.lang.Double] == midpointMillis)

    val allNull =
      agg.iterate(agg.init(), tupleOf("v", AttributeType.TIMESTAMP, null))
    assert(agg.finalAgg(allNull) == null)
  }

  // --- merge: the partial-combination lambda of each aggregation --------------

  // `AggregateOpSpec` drives init/iterate/finalAgg for every aggregation but only
  // ever merges AVERAGE/CONCAT partials. The `merge` lambdas of SUM, COUNT, MIN
  // and MAX are what the global stage calls when several workers report in, so
  // each one is exercised here directly and cross-checked against the equivalent
  // single-pass aggregation.

  private def aggregateAll(
      agg: DistributedAggregation[Object],
      t: AttributeType,
      values: Seq[AnyRef]
  ): Object =
    agg.finalAgg(values.map(v => tupleOf("v", t, v)).foldLeft(agg.init())(agg.iterate))

  "SUM aggregation merge" should "add two partials, matching a single-pass SUM" in {
    val agg = op(AggregationFunction.SUM).getAggFunc(AttributeType.LONG)
    val left = Seq[AnyRef](Long.box(1L), Long.box(2L))
    val right = Seq[AnyRef](Long.box(10L), Long.box(20L))

    val p1 = left.map(v => tupleOf("v", AttributeType.LONG, v)).foldLeft(agg.init())(agg.iterate)
    val p2 = right.map(v => tupleOf("v", AttributeType.LONG, v)).foldLeft(agg.init())(agg.iterate)

    assert(agg.finalAgg(agg.merge(p1, p2)) == Long.box(33L))
    assert(agg.finalAgg(agg.merge(p1, p2)) == aggregateAll(agg, AttributeType.LONG, left ++ right))
    // merging with an untouched (zero) partial must be a no-op
    assert(agg.finalAgg(agg.merge(p1, agg.init())) == Long.box(3L))
  }

  "COUNT aggregation merge" should "add the per-worker counts" in {
    val agg = op(AggregationFunction.COUNT).getAggFunc(AttributeType.INTEGER)
    val p1 = Seq[AnyRef](Int.box(1), null, Int.box(3))
      .map(v => tupleOf("v", AttributeType.INTEGER, v))
      .foldLeft(agg.init())(agg.iterate)
    val p2 = Seq[AnyRef](Int.box(4))
      .map(v => tupleOf("v", AttributeType.INTEGER, v))
      .foldLeft(agg.init())(agg.iterate)

    // COUNT(v) skips the null, so 2 + 1 == 3
    assert(agg.finalAgg(agg.merge(p1, p2)) == Int.box(3))
    assert(agg.finalAgg(agg.merge(agg.init(), agg.init())) == Int.box(0))
  }

  "MIN and MAX aggregation merge" should "pick the smaller and larger partial respectively" in {
    val minAgg = op(AggregationFunction.MIN).getAggFunc(AttributeType.DOUBLE)
    val maxAgg = op(AggregationFunction.MAX).getAggFunc(AttributeType.DOUBLE)
    val left = Seq[AnyRef](Double.box(4.0), Double.box(9.0))
    val right = Seq[AnyRef](Double.box(-1.5), Double.box(2.0))

    def partialOf(agg: DistributedAggregation[Object], values: Seq[AnyRef]): Object =
      values.map(v => tupleOf("v", AttributeType.DOUBLE, v)).foldLeft(agg.init())(agg.iterate)

    assert(
      minAgg.finalAgg(minAgg.merge(partialOf(minAgg, left), partialOf(minAgg, right)))
        == Double.box(-1.5)
    )
    // merge must be symmetric
    assert(
      minAgg.finalAgg(minAgg.merge(partialOf(minAgg, right), partialOf(minAgg, left)))
        == Double.box(-1.5)
    )
    assert(
      maxAgg.finalAgg(maxAgg.merge(partialOf(maxAgg, left), partialOf(maxAgg, right)))
        == Double.box(9.0)
    )
    assert(
      maxAgg.finalAgg(maxAgg.merge(partialOf(maxAgg, right), partialOf(maxAgg, left)))
        == Double.box(9.0)
    )
  }

  "MIN aggregation merge" should "stay neutral when one side saw no values" in {
    val agg = op(AggregationFunction.MIN).getAggFunc(AttributeType.INTEGER)
    val seen = agg.iterate(agg.init(), tupleOf("v", AttributeType.INTEGER, Int.box(7)))

    // An empty partial is the sentinel maxValue, so it must lose the comparison.
    assert(agg.finalAgg(agg.merge(seen, agg.init())) == Int.box(7))
    assert(agg.finalAgg(agg.merge(agg.init(), seen)) == Int.box(7))
    // Two empty partials still finalize to null (no rows anywhere).
    assert(agg.finalAgg(agg.merge(agg.init(), agg.init())) == null)
  }

  "MAX aggregation merge" should "stay neutral when one side saw no values" in {
    val agg = op(AggregationFunction.MAX).getAggFunc(AttributeType.INTEGER)
    val seen = agg.iterate(agg.init(), tupleOf("v", AttributeType.INTEGER, Int.box(7)))

    // An empty partial is the sentinel minValue, so it must lose the comparison.
    assert(agg.finalAgg(agg.merge(seen, agg.init())) == Int.box(7))
    assert(agg.finalAgg(agg.merge(agg.init(), seen)) == Int.box(7))
    // Two empty partials still finalize to null (no rows anywhere).
    assert(agg.finalAgg(agg.merge(agg.init(), agg.init())) == null)
  }

  // --- CONCAT: a null first value seeds the partial with an empty string ------

  "CONCAT aggregation" should "swallow leading nulls but keep interior ones as empty slots" in {
    // AggregateOpSpec only ever concatenates a null in the middle of the stream.
    // A null on the very first tuple takes the `partial == ""` side of the branch
    // and leaves the partial empty, so — unlike an interior null — it does not
    // occupy a slot in the comma-joined output.
    val agg = op(AggregationFunction.CONCAT).getAggFunc(AttributeType.STRING)
    val result = Seq[AnyRef](null, "red", null, "blue")
      .map(v => tupleOf("v", AttributeType.STRING, v))
      .foldLeft(agg.init())(agg.iterate)

    assert(agg.finalAgg(result) == "red,,blue")
  }

  it should "return an empty string when every value is null" in {
    val agg = op(AggregationFunction.CONCAT).getAggFunc(AttributeType.STRING)
    val onlyNull = agg.iterate(agg.init(), tupleOf("v", AttributeType.STRING, null))

    assert(agg.finalAgg(onlyNull) == "")
  }

  it should "stringify non-string values it is pointed at" in {
    // CONCAT is schema-restricted to STRING columns in the UI, but the executor
    // only ever calls `.toString`, so an INTEGER column still concatenates.
    val agg = op(AggregationFunction.CONCAT).getAggFunc(AttributeType.STRING)
    val result = Seq[AnyRef](Int.box(1), Int.box(2))
      .map(v => tupleOf("v", AttributeType.INTEGER, v))
      .foldLeft(agg.init())(agg.iterate)

    assert(agg.finalAgg(result) == "1,2")
  }

  // --- getAggregationAttribute / getFinal: message and identity guarantees ----

  "getAggregationAttribute" should "name the unknown aggregation function in its error" in {
    val ex = intercept[RuntimeException](op(null).getAggregationAttribute(AttributeType.INTEGER))
    assert(ex.getMessage == "Unknown aggregation function: null")
  }

  "getFinal" should "produce a detached copy that re-reads the result column" in {
    val original = op(AggregationFunction.MAX, attribute = "src", resultAttribute = "dst")
    val copy = original.getFinal

    assert(copy ne original)
    assert(copy.aggFunction == AggregationFunction.MAX)
    // the final stage reads and writes the same (result) column
    assert(copy.attribute == "dst")
    assert(copy.resultAttribute == "dst")
    // the original is left untouched
    assert(original.attribute == "src")
  }
}
