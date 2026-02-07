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
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.funsuite.AnyFunSuite

class AggregateOpSpec extends AnyFunSuite {

  /** Helpers */

  private def makeAggregationOp(
      fn: AggregationFunction,
      attributeName: String,
      resultName: String
  ): AggregationOperation = {
    val operation = new AggregationOperation()
    operation.aggFunction = fn
    operation.attribute = attributeName
    operation.resultAttribute = resultName
    operation
  }

  private def makeSchema(fields: (String, AttributeType)*): Schema =
    Schema(fields.map { case (n, t) => new Attribute(n, t) }.toList)

  private def makeTuple(schema: Schema, values: Any*): Tuple =
    Tuple(schema, values.toArray)

  test("getAggregationAttribute keeps original type for SUM") {
    val operation = makeAggregationOp(AggregationFunction.SUM, "amount", "total_amount")
    val attr = operation.getAggregationAttribute(AttributeType.DOUBLE)

    assert(attr.getName == "total_amount")
    assert(attr.getType == AttributeType.DOUBLE)
  }

  test("getAggregationAttribute maps COUNT result to INTEGER regardless of input type") {
    val operation = makeAggregationOp(AggregationFunction.COUNT, "quantity", "row_count")
    val attr = operation.getAggregationAttribute(AttributeType.LONG)

    assert(attr.getName == "row_count")
    assert(attr.getType == AttributeType.INTEGER)
  }

  test("getAggregationAttribute maps CONCAT result type to STRING") {
    val operation = makeAggregationOp(AggregationFunction.CONCAT, "tag", "all_tags")
    val attr = operation.getAggregationAttribute(AttributeType.INTEGER)

    assert(attr.getName == "all_tags")
    assert(attr.getType == AttributeType.STRING)
  }

  // ---------------------------------------------------------------------------
  // Basic DistributedAggregation behaviour via AggregationOperation.getAggFunc
  // ---------------------------------------------------------------------------

  test("SUM aggregation over INTEGER column adds values correctly") {
    val schema = makeSchema("amount" -> AttributeType.INTEGER)
    val tuple1 = makeTuple(schema, 5)
    val tuple2 = makeTuple(schema, 7)
    val tuple3 = makeTuple(schema, 3)

    val operation = makeAggregationOp(AggregationFunction.SUM, "amount", "total_amount")
    val agg = operation.getAggFunc(AttributeType.INTEGER)

    var partial = agg.init()
    partial = agg.iterate(partial, tuple1)
    partial = agg.iterate(partial, tuple2)
    partial = agg.iterate(partial, tuple3)

    val result = agg.finalAgg(partial).asInstanceOf[Number].intValue()
    assert(result == 15)
  }

  test("SUM aggregation over DOUBLE column keeps fractional part") {
    val schema = makeSchema("score" -> AttributeType.DOUBLE)
    val tuple1 = makeTuple(schema, 1.25)
    val tuple2 = makeTuple(schema, 2.75)

    val operation = makeAggregationOp(AggregationFunction.SUM, "score", "total_score")
    val agg = operation.getAggFunc(AttributeType.DOUBLE)

    var partial = agg.init()
    partial = agg.iterate(partial, tuple1)
    partial = agg.iterate(partial, tuple2)

    val result = agg.finalAgg(partial).asInstanceOf[java.lang.Double].doubleValue()
    assert(math.abs(result - 4.0) < 1e-6)
  }

  test("COUNT aggregation with attribute == null counts all rows") {
    val schema = makeSchema("points" -> AttributeType.INTEGER)
    val tuple1 = makeTuple(schema, 10)
    val tuple2 = makeTuple(schema, null)
    val tuple3 = makeTuple(schema, 20)

    val operation = makeAggregationOp(AggregationFunction.COUNT, null, "row_count")
    val agg = operation.getAggFunc(AttributeType.INTEGER)

    var partial = agg.init()
    partial = agg.iterate(partial, tuple1)
    partial = agg.iterate(partial, tuple2)
    partial = agg.iterate(partial, tuple3)

    val result = agg.finalAgg(partial).asInstanceOf[Number].intValue()
    assert(result == 3)
  }

  test("COUNT aggregation with attribute set only counts non-null values") {
    val schema = makeSchema("points" -> AttributeType.INTEGER)
    val tuple1 = makeTuple(schema, 10)
    val tuple2 = makeTuple(schema, null)
    val tuple3 = makeTuple(schema, 5)

    val operation = makeAggregationOp(AggregationFunction.COUNT, "points", "non_null_points")
    val agg = operation.getAggFunc(AttributeType.INTEGER)

    var partial = agg.init()
    partial = agg.iterate(partial, tuple1)
    partial = agg.iterate(partial, tuple2)
    partial = agg.iterate(partial, tuple3)

    val result = agg.finalAgg(partial).asInstanceOf[Number].intValue()
    assert(result == 2)
  }

  test("CONCAT aggregation concatenates string representations with commas") {
    val schema = makeSchema("tag" -> AttributeType.STRING)
    val tuple1 = makeTuple(schema, "red")
    val tuple2 = makeTuple(schema, null)
    val tuple3 = makeTuple(schema, "blue")

    val operation = makeAggregationOp(AggregationFunction.CONCAT, "tag", "all_tags")
    val agg = operation.getAggFunc(AttributeType.STRING)

    var partial = agg.init()
    partial = agg.iterate(partial, tuple1)
    partial = agg.iterate(partial, tuple2)
    partial = agg.iterate(partial, tuple3)

    val result = agg.finalAgg(partial).asInstanceOf[String]
    assert(result == "red,,blue")
  }

  test("MIN aggregation finds smallest INTEGER and returns null when given no values") {
    val schema = makeSchema("temperature" -> AttributeType.INTEGER)
    val tuple1 = makeTuple(schema, 10)
    val tuple2 = makeTuple(schema, -2)
    val tuple3 = makeTuple(schema, 5)

    val operation = makeAggregationOp(AggregationFunction.MIN, "temperature", "min_temp")
    val agg = operation.getAggFunc(AttributeType.INTEGER)

    // Empty case: never iterate, just finalize init
    val emptyPartial = agg.init()
    val emptyResult = agg.finalAgg(emptyPartial)
    assert(emptyResult == null)

    // Non-empty case
    var partial = agg.init()
    partial = agg.iterate(partial, tuple1)
    partial = agg.iterate(partial, tuple2)
    partial = agg.iterate(partial, tuple3)

    val result = agg.finalAgg(partial).asInstanceOf[Number].intValue()
    assert(result == -2)
  }

  test("MAX aggregation finds largest LONG value") {
    val schema = makeSchema("latency" -> AttributeType.LONG)
    val tuple1 = makeTuple(schema, 100L)
    val tuple2 = makeTuple(schema, 50L)
    val tuple3 = makeTuple(schema, 250L)

    val operation = makeAggregationOp(AggregationFunction.MAX, "latency", "max_latency")
    val agg = operation.getAggFunc(AttributeType.LONG)

    var partial = agg.init()
    partial = agg.iterate(partial, tuple1)
    partial = agg.iterate(partial, tuple2)
    partial = agg.iterate(partial, tuple3)

    val result = agg.finalAgg(partial).asInstanceOf[java.lang.Long].longValue()
    assert(result == 250L)
  }

  test(
    "MIN aggregation (DOUBLE) is solid: empty/null/NaN, infinities, signed zero, and many values"
  ) {
    val schema = makeSchema("temperature" -> AttributeType.DOUBLE)

    val operation = makeAggregationOp(AggregationFunction.MIN, "temperature", "min_temp")
    val agg = operation.getAggFunc(AttributeType.DOUBLE)

    // -----------------------
    // 0) Empty input => null
    // -----------------------
    val emptyPartial = agg.init()
    val emptyResult = agg.finalAgg(emptyPartial)
    assert(emptyResult == null)

    // ---------------------------------------------------------
    // 1) Only nulls / only NaNs => null
    // ---------------------------------------------------------
    val onlyNulls = Seq(makeTuple(schema, null), makeTuple(schema, null), makeTuple(schema, null))
    var partialNulls = agg.init()
    onlyNulls.foreach(tp => partialNulls = agg.iterate(partialNulls, tp))
    assert(agg.finalAgg(partialNulls) == null)

    val onlyNaNs = Seq(makeTuple(schema, Double.NaN), makeTuple(schema, Double.NaN))
    var partialNans = agg.init()
    onlyNaNs.foreach(tp => partialNans = agg.iterate(partialNans, tp))
    assert(agg.finalAgg(partialNans) == null)

    // ---------------------------------------------------------
    // 2) Basic decimals + negatives: should find the true minimum
    // ---------------------------------------------------------
    val basics = Seq(
      makeTuple(schema, 10.25),
      makeTuple(schema, -2.5),
      makeTuple(schema, 5.0),
      makeTuple(schema, -2.5000000001), // slightly smaller than -2.5
      makeTuple(schema, 1.0e-12)
    )

    var partial = agg.init()
    basics.foreach(tp => partial = agg.iterate(partial, tp))

    val basicResult = agg.finalAgg(partial).asInstanceOf[Number].doubleValue()
    assert(basicResult == -2.5000000001)

    // ---------------------------------------------------------
    // 3) NaN + null interleaving must not poison the result
    //    (especially if NaN appears first)
    // ---------------------------------------------------------
    val mixed = Seq(
      makeTuple(schema, Double.NaN),
      makeTuple(schema, null),
      makeTuple(schema, 3.14159),
      makeTuple(schema, -0.125),
      makeTuple(schema, Double.NaN),
      makeTuple(schema, -9999.0),
      makeTuple(schema, null)
    )

    var partialMixed = agg.init()
    mixed.foreach(tp => partialMixed = agg.iterate(partialMixed, tp))

    val mixedResult = agg.finalAgg(partialMixed).asInstanceOf[Number].doubleValue()
    assert(mixedResult == -9999.0)

    // ---------------------------------------------------------
    // 4) Infinities: min should be -Infinity if present
    // ---------------------------------------------------------
    val infinities = Seq(
      makeTuple(schema, Double.PositiveInfinity),
      makeTuple(schema, 42.0),
      makeTuple(schema, Double.NegativeInfinity),
      makeTuple(schema, -1.0)
    )

    var partialInf = agg.init()
    infinities.foreach(tp => partialInf = agg.iterate(partialInf, tp))

    val infResult = agg.finalAgg(partialInf).asInstanceOf[Number].doubleValue()
    assert(infResult.isNegInfinity)

    // ---------------------------------------------------------
    // 5) Signed zero: MIN(-0.0, +0.0) should be -0.0
    // ---------------------------------------------------------
    val signedZero = Seq(makeTuple(schema, 0.0), makeTuple(schema, -0.0), makeTuple(schema, 0.0))
    var partialZero = agg.init()
    signedZero.foreach(tp => partialZero = agg.iterate(partialZero, tp))

    val zeroValue = agg.finalAgg(partialZero).asInstanceOf[Number].doubleValue()
    val zeroBits = java.lang.Double.doubleToRawLongBits(zeroValue)
    val negZeroBits = java.lang.Double.doubleToRawLongBits(-0.0)
    assert(zeroBits == negZeroBits)

    // ---------------------------------------------------------
    // 6) Stress test: many values, compare against a reference min
    //    Reference rule here: ignore null and NaN; return null if none left.
    // ---------------------------------------------------------
    val rng = new scala.util.Random(1337)
    val values: Seq[java.lang.Double] =
      (1 to 10000).map { index =>
        index % 250 match {
          case 0 => null
          case 1 => Double.NaN
          case 2 => Double.PositiveInfinity
          case 3 => Double.NegativeInfinity
          case 4 => -0.0
          case _ =>
            // wide-ish range with some tiny magnitudes too
            val sign = if (rng.nextBoolean()) 1.0 else -1.0
            sign * (rng.nextDouble() * 1.0e6) / (if (rng.nextInt(20) == 0) 1.0e12 else 1.0)
        }
      }

    val expected: java.lang.Double = {
      var found = false
      var currentMin = 0.0
      values.foreach { x =>
        if (x != null && !java.lang.Double.isNaN(x)) {
          if (!found) {
            currentMin = x; found = true
          } else if (java.lang.Double.compare(x, currentMin) < 0) currentMin = x
        }
      }
      if (!found) null else currentMin
    }

    var partStress = agg.init()
    values.foreach(v => partStress = agg.iterate(partStress, makeTuple(schema, v)))
    val gotAny = agg.finalAgg(partStress)

    if (expected == null) {
      assert(gotAny == null)
    } else {
      val got = gotAny.asInstanceOf[Number].doubleValue()
      // exact match: should be one of the seen inputs, no tolerance needed
      if (
        expected == 0.0 && java.lang.Double.doubleToRawLongBits(expected) != java.lang.Double
          .doubleToRawLongBits(got)
      ) {
        // If expected is -0.0, enforce it
        assert(
          java.lang.Double.doubleToRawLongBits(got) == java.lang.Double.doubleToRawLongBits(
            expected
          )
        )
      } else {
        assert(got == expected)
      }
    }
  }

  test("MAX aggregation finds largest DOUBLE value") {
    val maxValue = 99.144
    val schema = makeSchema("debt" -> AttributeType.DOUBLE)
    val tuple1 = makeTuple(schema, -100.123)
    val tuple2 = makeTuple(schema, 50.12)
    val tuple3 = makeTuple(schema, maxValue)

    val operation = makeAggregationOp(AggregationFunction.MAX, "debt", "nax_debt")
    val agg = operation.getAggFunc(AttributeType.DOUBLE)

    var partial = agg.init()
    partial = agg.iterate(partial, tuple1)
    partial = agg.iterate(partial, tuple2)
    partial = agg.iterate(partial, tuple3)

    val result = agg.finalAgg(partial).asInstanceOf[java.lang.Double].doubleValue()
    assert(result == maxValue)
  }

  test("AVERAGE aggregation ignores nulls and returns null when all values are null") {
    val schema = makeSchema("price" -> AttributeType.DOUBLE)
    val tuple1 = makeTuple(schema, 10.0)
    val tuple2 = makeTuple(schema, null)
    val tuple3 = makeTuple(schema, 20.0)

    val operation = makeAggregationOp(AggregationFunction.AVERAGE, "price", "avg_price")
    val agg = operation.getAggFunc(AttributeType.DOUBLE)

    // Mixed null and non-null
    var partial = agg.init()
    partial = agg.iterate(partial, tuple1)
    partial = agg.iterate(partial, tuple2)
    partial = agg.iterate(partial, tuple3)

    val avg = agg.finalAgg(partial).asInstanceOf[java.lang.Double].doubleValue()
    assert(math.abs(avg - 15.0) < 1e-6)

    // All nulls
    val allNull = makeTuple(schema, null)
    var partialAllNull = agg.init()
    partialAllNull = agg.iterate(partialAllNull, allNull)
    val allNullResult = agg.finalAgg(partialAllNull)
    assert(allNullResult == null)
  }

  // ---------------------------------------------------------------------------
  // getFinal behaviour
  // ---------------------------------------------------------------------------

  test("getFinal rewrites COUNT into SUM over the intermediate result attribute") {
    val operation = makeAggregationOp(AggregationFunction.COUNT, "price", "price_count")
    val finalOp = operation.getFinal

    assert(finalOp.aggFunction == AggregationFunction.SUM)
    assert(finalOp.attribute == "price_count")
    assert(finalOp.resultAttribute == "price_count")
  }

  test("getFinal keeps non-COUNT aggregation function and rewires attribute to resultAttribute") {
    val operation = makeAggregationOp(AggregationFunction.SUM, "amount", "total_amount")
    val finalOp = operation.getFinal

    assert(finalOp.aggFunction == AggregationFunction.SUM)
    assert(finalOp.attribute == "total_amount")
    assert(finalOp.resultAttribute == "total_amount")
  }

  // ---------------------------------------------------------------------------
  // AggregateOpExec: integration-style tests with groupBy
  // ---------------------------------------------------------------------------

  test("AggregateOpExec groups by a single key and computes SUM per group") {
    // schema: city (group key), sales
    val schema = makeSchema(
      "city" -> AttributeType.STRING,
      "sales" -> AttributeType.INTEGER
    )

    val tuple1 = makeTuple(schema, "NY", 10)
    val tuple2 = makeTuple(schema, "SF", 20)
    val tuple3 = makeTuple(schema, "NY", 5)

    val desc = new AggregateOpDesc()
    val sumAgg = makeAggregationOp(AggregationFunction.SUM, "sales", "total_sales")
    desc.aggregations = List(sumAgg)
    desc.groupByKeys = List("city")

    val descJson = objectMapper.writeValueAsString(desc)

    val exec = new AggregateOpExec(descJson)
    exec.open()
    exec.processTuple(tuple1, 0)
    exec.processTuple(tuple2, 0)
    exec.processTuple(tuple3, 0)

    val results = exec.onFinish(0).toList

    // Expect two output rows: (NY, 15) and (SF, 20)
    val resultMap = results.map { tupleLike =>
      val fields = tupleLike.getFields
      val city = fields(0).asInstanceOf[String]
      val total = fields(1).asInstanceOf[Number].intValue()
      city -> total
    }.toMap

    assert(resultMap.size == 2)
    assert(resultMap("NY") == 15)
    assert(resultMap("SF") == 20)
  }

  test("AggregateOpExec performs global SUM and COUNT when there are no groupBy keys") {
    // schema: region (ignored for aggregation), revenue
    val schema = makeSchema(
      "region" -> AttributeType.STRING,
      "revenue" -> AttributeType.INTEGER
    )

    val tuple1 = makeTuple(schema, "west", 100)
    val tuple2 = makeTuple(schema, "east", 200)
    val tuple3 = makeTuple(schema, "west", 50)

    val desc = new AggregateOpDesc()
    val sumAgg = makeAggregationOp(AggregationFunction.SUM, "revenue", "total_revenue")
    val countAgg = makeAggregationOp(AggregationFunction.COUNT, "revenue", "row_count")
    desc.aggregations = List(sumAgg, countAgg)
    desc.groupByKeys = List() // global aggregation

    val descJson = objectMapper.writeValueAsString(desc)

    val exec = new AggregateOpExec(descJson)
    exec.open()
    exec.processTuple(tuple1, 0)
    exec.processTuple(tuple2, 0)
    exec.processTuple(tuple3, 0)

    val results = exec.onFinish(0).toList
    assert(results.size == 1)

    val fields = results.head.getFields
    // No group keys, so fields(0) is SUM(revenue), fields(1) is COUNT(revenue)
    val totalRevenue = fields(0).asInstanceOf[Number].intValue()
    val rowCount = fields(1).asInstanceOf[Number].intValue()

    assert(totalRevenue == 350)
    assert(rowCount == 3)
  }
}
