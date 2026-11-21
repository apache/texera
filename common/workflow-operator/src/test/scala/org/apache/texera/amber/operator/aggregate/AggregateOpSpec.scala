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

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp

class AggregateOpSpec extends AnyFunSuite {
  import AggregationTestSupport._

  test("getAggregationAttribute keeps original type for SUM") {
    assertAggregationAttribute(
      AggregationFunction.SUM,
      "amount",
      "total_amount",
      AttributeType.DOUBLE,
      AttributeType.DOUBLE
    )
  }

  test("getAggregationAttribute maps COUNT result to INTEGER regardless of input type") {
    assertAggregationAttribute(
      AggregationFunction.COUNT,
      "quantity",
      "row_count",
      AttributeType.LONG,
      AttributeType.INTEGER
    )
  }

  test("getAggregationAttribute maps COUNT result to INTEGER even with a null input type") {
    // COUNT(*) (empty attribute) has no input column, so schema propagation passes a
    // null attrType; it must still resolve to INTEGER without dereferencing it.
    assertAggregationAttribute(
      AggregationFunction.COUNT,
      "",
      "row_count",
      null,
      AttributeType.INTEGER
    )
  }

  test("getAggregationAttribute maps CONCAT result type to STRING") {
    assertAggregationAttribute(
      AggregationFunction.CONCAT,
      "tag",
      "all_tags",
      AttributeType.INTEGER,
      AttributeType.STRING
    )
  }

  test("getAggregationAttribute maps AVERAGE result type to DOUBLE regardless of input") {
    assertAggregationAttribute(
      AggregationFunction.AVERAGE,
      "price",
      "avg_price",
      AttributeType.LONG,
      AttributeType.DOUBLE
    )
  }

  test("getAggregationAttribute keeps original type for MIN") {
    assertAggregationAttribute(
      AggregationFunction.MIN,
      "ts",
      "min_ts",
      AttributeType.TIMESTAMP,
      AttributeType.TIMESTAMP
    )
  }

  test("getAggregationAttribute keeps original type for MAX") {
    assertAggregationAttribute(
      AggregationFunction.MAX,
      "score",
      "max_score",
      AttributeType.DOUBLE,
      AttributeType.DOUBLE
    )
  }

  test("getAggregationAttribute throws RuntimeException when aggFunction is null") {
    val operation = makeAggregationOp(null, "src", "out")
    assertThrows[RuntimeException] {
      operation.getAggregationAttribute(AttributeType.INTEGER)
    }
  }

  test("SUM aggregation over INTEGER column adds values correctly") {
    val result =
      runSingleAttrAgg(AggregationFunction.SUM, "amount", AttributeType.INTEGER, Seq(5, 7, 3))
    assert(result.asInstanceOf[Number].intValue() == 15)
  }

  test("SUM aggregation over DOUBLE column keeps fractional part") {
    val result =
      runSingleAttrAgg(AggregationFunction.SUM, "score", AttributeType.DOUBLE, Seq(1.25, 2.75))
    assert(math.abs(result.asInstanceOf[java.lang.Double].doubleValue() - 4.0) < 1e-6)
  }

  test("COUNT with an empty attribute (COUNT(*)) counts all rows regardless of nulls") {
    val result =
      runSingleAttrAgg(AggregationFunction.COUNT, "", AttributeType.INTEGER, Seq(10, null, 20))
    assert(result.asInstanceOf[Number].intValue() == 3)
  }

  test("COUNT with a null attribute also counts all rows") {
    val result =
      runSingleAttrAgg(AggregationFunction.COUNT, null, AttributeType.INTEGER, Seq(10, null, 20))
    assert(result.asInstanceOf[Number].intValue() == 3)
  }

  test("COUNT aggregation with attribute set only counts non-null values") {
    val result =
      runSingleAttrAgg(AggregationFunction.COUNT, "points", AttributeType.INTEGER, Seq(10, null, 5))
    assert(result.asInstanceOf[Number].intValue() == 2)
  }

  test("CONCAT aggregation concatenates string representations with commas") {
    val result = runSingleAttrAgg(
      AggregationFunction.CONCAT,
      "tag",
      AttributeType.STRING,
      Seq("red", null, "blue")
    )
    assert(result.asInstanceOf[String] == "red,,blue")
  }

  test("MIN aggregation finds smallest INTEGER and returns null when given no values") {
    assert(
      runSingleAttrAgg(
        AggregationFunction.MIN,
        "temperature",
        AttributeType.INTEGER,
        Seq.empty
      ) == null
    )
    val result = runSingleAttrAgg(
      AggregationFunction.MIN,
      "temperature",
      AttributeType.INTEGER,
      Seq(10, -2, 5)
    )
    assert(result.asInstanceOf[Number].intValue() == -2)
  }

  test("MAX aggregation finds largest LONG value") {
    val result =
      runSingleAttrAgg(AggregationFunction.MAX, "latency", AttributeType.LONG, Seq(100L, 50L, 250L))
    assert(result.asInstanceOf[java.lang.Long].longValue() == 250L)
  }

  test(
    "MIN aggregation (DOUBLE) is solid: empty/null/NaN, infinities, signed zero, and many values"
  ) {
    val schema = makeSchema("temperature" -> AttributeType.DOUBLE)
    val agg = makeAggregationOp(AggregationFunction.MIN, "temperature", "min_temp")
      .getAggFunc(AttributeType.DOUBLE)

    assert(agg.finalAgg(agg.init()) == null)
    assert(
      runSingleAttrAgg(
        AggregationFunction.MIN,
        "temperature",
        AttributeType.DOUBLE,
        Seq(null, null, null)
      ) == null
    )
    assert(
      runSingleAttrAgg(
        AggregationFunction.MIN,
        "temperature",
        AttributeType.DOUBLE,
        Seq(Double.NaN, Double.NaN)
      ) == null
    )

    val basicResult = runSingleAttrAgg(
      AggregationFunction.MIN,
      "temperature",
      AttributeType.DOUBLE,
      Seq(10.25, -2.5, 5.0, -2.5000000001, 1.0e-12)
    ).asInstanceOf[Number].doubleValue()
    assert(basicResult == -2.5000000001)

    val mixedResult = runSingleAttrAgg(
      AggregationFunction.MIN,
      "temperature",
      AttributeType.DOUBLE,
      Seq(Double.NaN, null, 3.14159, -0.125, Double.NaN, -9999.0, null)
    ).asInstanceOf[Number].doubleValue()
    assert(mixedResult == -9999.0)

    val infResult = runSingleAttrAgg(
      AggregationFunction.MIN,
      "temperature",
      AttributeType.DOUBLE,
      Seq(Double.PositiveInfinity, 42.0, Double.NegativeInfinity, -1.0)
    ).asInstanceOf[Number].doubleValue()
    assert(infResult.isNegInfinity)

    val zeroValue = runSingleAttrAgg(
      AggregationFunction.MIN,
      "temperature",
      AttributeType.DOUBLE,
      Seq(0.0, -0.0, 0.0)
    ).asInstanceOf[Number].doubleValue()
    assert(
      java.lang.Double.doubleToRawLongBits(zeroValue) == java.lang.Double.doubleToRawLongBits(-0.0)
    )

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
      if (
        expected == 0.0 && java.lang.Double.doubleToRawLongBits(expected) != java.lang.Double
          .doubleToRawLongBits(got)
      ) {
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
    val result = runSingleAttrAgg(
      AggregationFunction.MAX,
      "debt",
      AttributeType.DOUBLE,
      Seq(-100.123, 50.12, maxValue)
    )
    assert(result.asInstanceOf[java.lang.Double].doubleValue() == maxValue)
  }

  test("MAX aggregation finds largest INTEGER and returns null when given no values") {
    assert(
      runSingleAttrAgg(
        AggregationFunction.MAX,
        "temperature",
        AttributeType.INTEGER,
        Seq.empty
      ) == null
    )
    val result = runSingleAttrAgg(
      AggregationFunction.MAX,
      "temperature",
      AttributeType.INTEGER,
      Seq(10, -2, 5)
    )
    assert(result.asInstanceOf[Number].intValue() == 10)
  }

  test("MAX aggregation returns null when all values are null") {
    assert(
      runSingleAttrAgg(
        AggregationFunction.MAX,
        "temperature",
        AttributeType.INTEGER,
        Seq.fill(3)(null)
      ) == null
    )
  }

  test("MAX aggregation keeps the type's maximum value when it is the true maximum") {
    val cases: Seq[(AttributeType, Seq[Any], Any)] = Seq(
      (AttributeType.INTEGER, Seq(1, 5, Int.MaxValue), Int.MaxValue),
      (AttributeType.LONG, Seq(1L, 5L, Long.MaxValue), Long.MaxValue),
      (AttributeType.DOUBLE, Seq(1.0, 5.0, Double.MaxValue), Double.MaxValue),
      (
        AttributeType.TIMESTAMP,
        Seq(new Timestamp(1L), new Timestamp(Long.MaxValue)),
        new Timestamp(Long.MaxValue)
      )
    )
    for ((attrType, values, expected) <- cases) {
      assert(
        runSingleAttrAgg(AggregationFunction.MAX, "v", attrType, values) == expected,
        s"MAX over $attrType must keep $expected"
      )
    }
  }

  test("AVERAGE aggregation ignores nulls and returns null when all values are null") {
    val avg = runSingleAttrAgg(
      AggregationFunction.AVERAGE,
      "price",
      AttributeType.DOUBLE,
      Seq(10.0, null, 20.0)
    )
    assert(math.abs(avg.asInstanceOf[java.lang.Double].doubleValue() - 15.0) < 1e-6)
    assert(
      runSingleAttrAgg(
        AggregationFunction.AVERAGE,
        "price",
        AttributeType.DOUBLE,
        Seq(null)
      ) == null
    )
  }

  test("getFinal rewrites COUNT into SUM over the intermediate result attribute") {
    val finalOp = makeAggregationOp(AggregationFunction.COUNT, "price", "price_count").getFinal
    assert(finalOp.aggFunction == AggregationFunction.SUM)
    assert(finalOp.attribute == "price_count" && finalOp.resultAttribute == "price_count")
  }

  test("getFinal keeps non-COUNT aggregation function and rewires attribute to resultAttribute") {
    val finalOp = makeAggregationOp(AggregationFunction.SUM, "amount", "total_amount").getFinal
    assert(finalOp.aggFunction == AggregationFunction.SUM)
    assert(finalOp.attribute == "total_amount" && finalOp.resultAttribute == "total_amount")
  }

  test("AggregateOpExec groups by a single key and computes SUM per group") {
    val schema = makeSchema("city" -> AttributeType.STRING, "sales" -> AttributeType.INTEGER)
    val exec = openAggregateExec(
      List(makeAggregationOp(AggregationFunction.SUM, "sales", "total_sales")),
      List("city")
    )
    val results = runAggregateExec(
      exec,
      makeTuple(schema, "NY", 10),
      makeTuple(schema, "SF", 20),
      makeTuple(schema, "NY", 5)
    )
    val resultMap = results.map { tupleLike =>
      val fields = tupleLike.getFields
      fields(0).asInstanceOf[String] -> fields(1).asInstanceOf[Number].intValue()
    }.toMap
    assert(resultMap == Map("NY" -> 15, "SF" -> 20))
  }

  test("AggregateOpExec performs global SUM and COUNT when there are no groupBy keys") {
    val schema = makeSchema("region" -> AttributeType.STRING, "revenue" -> AttributeType.INTEGER)
    val exec = openAggregateExec(
      List(
        makeAggregationOp(AggregationFunction.SUM, "revenue", "total_revenue"),
        makeAggregationOp(AggregationFunction.COUNT, "revenue", "row_count")
      ),
      List()
    )
    val results = runAggregateExec(
      exec,
      makeTuple(schema, "west", 100),
      makeTuple(schema, "east", 200),
      makeTuple(schema, "west", 50)
    )
    assert(results.size == 1)
    val fields = results.head.getFields
    assert(fields(0).asInstanceOf[Number].intValue() == 350)
    assert(fields(1).asInstanceOf[Number].intValue() == 3)
  }

  test("AggregateOpExec computes COUNT(*) over every row (including nulls) end-to-end") {
    val schema = makeSchema("region" -> AttributeType.STRING, "revenue" -> AttributeType.INTEGER)
    val exec = openAggregateExec(
      List(makeAggregationOp(AggregationFunction.COUNT, "", "row_count")),
      List()
    )
    val results = runAggregateExec(
      exec,
      makeTuple(schema, "west", 100),
      makeTuple(schema, "east", null),
      makeTuple(schema, "west", 50)
    )
    assert(results.size == 1 && results.head.getFields(0).asInstanceOf[Number].intValue() == 3)
  }

  test("AggregateOpExec close() discards the accumulated groups") {
    val schema = makeSchema("city" -> AttributeType.STRING, "sales" -> AttributeType.INTEGER)
    val exec = openAggregateExec(
      List(makeAggregationOp(AggregationFunction.SUM, "sales", "total_sales")),
      List("city")
    )
    assert(
      runAggregateExec(exec, makeTuple(schema, "NY", 10), makeTuple(schema, "SF", 20)).size == 2
    )
    exec.close()
    assert(exec.onFinish(0).toList.isEmpty)
  }

  test("AggregateOpExec open() after close() accumulates from an empty state") {
    val schema = makeSchema("city" -> AttributeType.STRING, "sales" -> AttributeType.INTEGER)
    val exec = openAggregateExec(
      List(makeAggregationOp(AggregationFunction.SUM, "sales", "total_sales")),
      List("city")
    )
    exec.processTuple(makeTuple(schema, "NY", 10), 0)
    exec.close()

    exec.open()
    val results = runAggregateExec(exec, makeTuple(schema, "NY", 3))
    assert(results.size == 1 && results.head.getFields(1).asInstanceOf[Number].intValue() == 3)
  }

  test("AggregateOpExec columnar accumulate matches the row path (grouped SUM/COUNT/AVG/MIN/MAX)") {
    // The native-Arrow accumulate (processColumnarBatch) must produce the same
    // grouped result as feeding the same rows through processTuple, so enabling the
    // columnar wire never changes an aggregate's output.
    val schema = makeSchema(
      "city" -> AttributeType.STRING,
      "sales" -> AttributeType.INTEGER
    )
    val rows = Seq(
      makeTuple(schema, "NY", 10),
      makeTuple(schema, "SF", 20),
      makeTuple(schema, "NY", 5),
      makeTuple(schema, "SF", 7),
      makeTuple(schema, "NY", null) // a null value: counted by COUNT(*) only, skipped by the rest
    )

    val desc = new AggregateOpDesc()
    desc.aggregations = List(
      makeAggregationOp(AggregationFunction.SUM, "sales", "sum_sales"),
      makeAggregationOp(AggregationFunction.COUNT, "", "n_rows"),
      makeAggregationOp(AggregationFunction.AVERAGE, "sales", "avg_sales"),
      makeAggregationOp(AggregationFunction.MIN, "sales", "min_sales"),
      makeAggregationOp(AggregationFunction.MAX, "sales", "max_sales")
    )
    desc.groupByKeys = List("city")
    val descJson = objectMapper.writeValueAsString(desc)

    def resultsOf(feed: AggregateOpExec => Unit): Map[String, List[Any]] = {
      val exec = new AggregateOpExec(descJson)
      exec.open()
      feed(exec)
      val out = exec
        .onFinish(0)
        .toList
        .map { t =>
          val f = t.getFields
          f(0).asInstanceOf[String] -> f.drop(1).toList
        }
        .toMap
      exec.close()
      out
    }

    val rowResult = resultsOf(exec => rows.foreach(exec.processTuple(_, 0)))
    val colResult =
      resultsOf(exec =>
        exec.processColumnarBatch(ArrowUtils.serializeTuples(schema, rows.toArray), 0)
      )

    assert(colResult.keySet == rowResult.keySet)
    colResult.keys.foreach { city =>
      assert(colResult(city) == rowResult(city), s"group $city differs")
    }
  }

  test("processColumnarRoot accumulates a live root identically to processColumnarBatch") {
    val schema = makeSchema("city" -> AttributeType.STRING, "sales" -> AttributeType.INTEGER)
    val rows = Seq(
      makeTuple(schema, "NY", 10),
      makeTuple(schema, "SF", 20),
      makeTuple(schema, "NY", 5),
      makeTuple(schema, "SF", null)
    )
    val desc = new AggregateOpDesc()
    desc.aggregations = List(
      makeAggregationOp(AggregationFunction.SUM, "sales", "sum_sales"),
      makeAggregationOp(AggregationFunction.COUNT, "", "n_rows")
    )
    desc.groupByKeys = List("city")
    val descJson = objectMapper.writeValueAsString(desc)

    def resultsOf(feed: AggregateOpExec => Unit): Map[String, List[Any]] = {
      val exec = new AggregateOpExec(descJson)
      exec.open()
      feed(exec)
      val out = exec
        .onFinish(0)
        .toList
        .map { t =>
          val f = t.getFields; f(0).asInstanceOf[String] -> f.drop(1).toList
        }
        .toMap
      exec.close()
      out
    }

    val bytesResult = resultsOf(exec =>
      exec.processColumnarBatch(ArrowUtils.serializeTuples(schema, rows.toArray), 0)
    )
    val rootResult = resultsOf { exec =>
      val alloc = new org.apache.arrow.memory.RootAllocator()
      try {
        ArrowUtils.deserializeRootFold(ArrowUtils.serializeTuples(schema, rows.toArray), alloc)(
          exec.processColumnarRoot(_, 0)
        )
      } finally alloc.close()
    }
    assert(rootResult == bytesResult)
  }
}
