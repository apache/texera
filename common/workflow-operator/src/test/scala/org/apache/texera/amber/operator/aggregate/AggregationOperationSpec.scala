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
import org.scalatest.Assertions
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp

private[aggregate] object AggregationTestSupport extends Assertions {
  def makeAggregationOp(
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

  def makeSchema(fields: (String, AttributeType)*): Schema =
    Schema(fields.map { case (n, t) => new Attribute(n, t) }.toList)

  def makeTuple(schema: Schema, values: Any*): Tuple =
    Tuple(schema, values.toArray)

  def singleTuple(name: String, t: AttributeType, value: AnyRef): Tuple =
    Tuple.builder(makeSchema(name -> t)).add(new Attribute(name, t), value).build()

  def partialOf(
      agg: DistributedAggregation[Object],
      attr: String,
      t: AttributeType,
      values: Seq[Any]
  ): Object = {
    val schema = makeSchema(Option(attr).filter(_.nonEmpty).getOrElse("points") -> t)
    values.map(v => makeTuple(schema, v)).foldLeft(agg.init())(agg.iterate)
  }

  def runSingleAttrAgg(
      fn: AggregationFunction,
      attr: String,
      t: AttributeType,
      values: Seq[Any]
  ): Object = {
    val agg = makeAggregationOp(fn, attr, "result").getAggFunc(t)
    agg.finalAgg(partialOf(agg, attr, t, values))
  }

  def openAggregateExec(
      aggregations: List[AggregationOperation],
      groupByKeys: List[String]
  ): AggregateOpExec = {
    val desc = new AggregateOpDesc()
    desc.aggregations = aggregations
    desc.groupByKeys = groupByKeys
    val exec = new AggregateOpExec(objectMapper.writeValueAsString(desc))
    exec.open()
    exec
  }

  def runAggregateExec(
      exec: AggregateOpExec,
      tuples: Tuple*
  ): List[org.apache.texera.amber.core.tuple.TupleLike] = {
    tuples.foreach(exec.processTuple(_, 0))
    exec.onFinish(0).toList
  }

  def assertAggregationAttribute(
      fn: AggregationFunction,
      attribute: String,
      resultAttribute: String,
      inputType: AttributeType,
      expectedType: AttributeType
  ): Unit = {
    val attr = makeAggregationOp(fn, attribute, resultAttribute).getAggregationAttribute(inputType)
    assert(attr.getName == resultAttribute)
    assert(attr.getType == expectedType)
  }

  def assertNeutralMergeWithEmptyPartial(
      fn: AggregationFunction,
      sample: AnyRef,
      t: AttributeType = AttributeType.INTEGER
  ): Unit = {
    val agg = makeAggregationOp(fn, "v", "r").getAggFunc(t)
    val seen = agg.iterate(agg.init(), singleTuple("v", t, sample))
    assert(agg.finalAgg(agg.merge(seen, agg.init())) == sample)
    assert(agg.finalAgg(agg.merge(agg.init(), seen)) == sample)
    assert(agg.finalAgg(agg.merge(agg.init(), agg.init())) == null)
  }
}

/**
  * Coverage notes:
  * `AggregateOpSpec` (in this same package) already exercises the happy paths for
  * `getAggregationAttribute`, the per-kind `init` / `iterate` / `finalAgg` semantics,
  * and the `getFinal` rewrite. This spec exercises validation errors, merge semantics,
  * multi-stage `getFinal` worker->final pipelines, and timestamp averaging.
  */
class AggregationOperationSpec extends AnyFlatSpec {
  import AggregationTestSupport._

  private def op(
      func: AggregationFunction,
      attribute: String = "v",
      resultAttribute: String = "r"
  ): AggregationOperation = makeAggregationOp(func, attribute, resultAttribute)

  "AggregationOperation.getAggFunc" should "throw UnsupportedOperationException for unsupported attribute types on SUM" in {
    val ex = intercept[UnsupportedOperationException] {
      op(AggregationFunction.SUM).getAggFunc(AttributeType.STRING)
    }
    assert(ex.getMessage.contains("Unsupported attribute type for sum"))
  }

  it should "throw UnsupportedOperationException for unsupported attribute types on MIN and MAX" in {
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

  "CONCAT aggregation merge" should
    "join two non-empty partials with a comma and short-circuit when either is empty" in {
    val agg = op(AggregationFunction.CONCAT).getAggFunc(AttributeType.STRING)
    assert(agg.merge("foo", "bar") == "foo,bar")
    assert(agg.merge("", "bar") == "bar")
    assert(agg.merge("foo", "") == "foo")
    assert(agg.merge("", "") == "")
  }

  "Worker → final aggregation pipeline" should
    "give the same total as a single-pass COUNT when partials are re-aggregated via getFinal" in {
    val workerOp = op(AggregationFunction.COUNT, attribute = "v", resultAttribute = "row_count")
    val w1Out = runSingleAttrAgg(
      AggregationFunction.COUNT,
      "v",
      AttributeType.INTEGER,
      Seq(Int.box(10), null, Int.box(20))
    ).asInstanceOf[Integer]
    assert(w1Out == 2, "worker 1 saw two non-null values")

    val w2Out = runSingleAttrAgg(
      AggregationFunction.COUNT,
      "v",
      AttributeType.INTEGER,
      Seq(Int.box(30), Int.box(40), Int.box(50))
    ).asInstanceOf[Integer]
    assert(w2Out == 3)

    val finalOp = workerOp.getFinal
    assert(finalOp.aggFunction == AggregationFunction.SUM)
    assert(finalOp.attribute == "row_count")
    val finalAgg = finalOp.getAggFunc(AttributeType.INTEGER)
    assert(
      finalAgg.finalAgg(
        partialOf(finalAgg, "row_count", AttributeType.INTEGER, Seq(w1Out, w2Out))
      ) == 5
    )
  }

  it should
    "give the same total as a single-pass SUM when partials are re-aggregated via getFinal" in {
    val workerOp = op(AggregationFunction.SUM, attribute = "v", resultAttribute = "total")
    val workerAgg = workerOp.getAggFunc(AttributeType.INTEGER)
    val partials = Seq(
      Seq(Int.box(1), Int.box(2), Int.box(3)),
      Seq(Int.box(10), Int.box(20))
    ).map(vs =>
      workerAgg.finalAgg(partialOf(workerAgg, "v", AttributeType.INTEGER, vs)).asInstanceOf[Integer]
    )
    assert(partials == Seq(6: Integer, 30: Integer))

    val finalOp = workerOp.getFinal
    assert(finalOp.aggFunction == AggregationFunction.SUM)
    assert(finalOp.attribute == "total")
    val finalAgg = finalOp.getAggFunc(AttributeType.INTEGER)
    assert(
      finalAgg.finalAgg(partialOf(finalAgg, "total", AttributeType.INTEGER, partials)) == 36,
      "single-pass SUM(1+2+3+10+20) == 36"
    )
  }

  it should
    "keep the type's maximum value when partial MAX results are re-aggregated via getFinal" in {
    val workerOp = op(AggregationFunction.MAX, attribute = "v", resultAttribute = "max_v")
    val w1Out = runSingleAttrAgg(
      AggregationFunction.MAX,
      "v",
      AttributeType.INTEGER,
      Seq(Int.box(1), Int.box(Int.MaxValue))
    )
    assert(w1Out == Int.box(Int.MaxValue), "worker 1's true maximum is Int.MaxValue")

    val w2Out = runSingleAttrAgg(
      AggregationFunction.MAX,
      "v",
      AttributeType.INTEGER,
      Seq(Int.box(5), Int.box(2))
    )
    assert(w2Out == Int.box(5))

    val finalOp = workerOp.getFinal
    assert(finalOp.aggFunction == AggregationFunction.MAX)
    val finalAgg = finalOp.getAggFunc(AttributeType.INTEGER)
    assert(
      finalAgg.finalAgg(
        partialOf(finalAgg, "max_v", AttributeType.INTEGER, Seq(w1Out, w2Out))
      ) == Int
        .box(Int.MaxValue)
    )
  }

  "AveragePartialObj" should "expose its sum and count fields and support value equality" in {
    val a = AveragePartialObj(10.0, 4)
    val b = AveragePartialObj(10.0, 4)
    assert(a.sum == 10.0 && a.count == 4 && a == b && a.hashCode == b.hashCode)
  }

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
    val unsupported = Seq(AttributeType.STRING, AttributeType.BOOLEAN, AttributeType.BINARY)
    for ((func, name) <- guardedAggregations; t <- unsupported) {
      val ex = intercept[UnsupportedOperationException](op(func).getAggFunc(t))
      assert(ex.getMessage == s"Unsupported attribute type for $name aggregation: $t")
    }
  }

  private val earlier = Timestamp.valueOf("2020-03-05 10:00:00")
  private val later = Timestamp.valueOf("2020-03-05 11:00:00")
  private val midpointMillis = (earlier.getTime + later.getTime) / 2.0

  "AVERAGE over a TIMESTAMP column" should "average the values' epoch milliseconds" in {
    assert(
      runSingleAttrAgg(
        AggregationFunction.AVERAGE,
        "v",
        AttributeType.TIMESTAMP,
        Seq(earlier, later)
      ).asInstanceOf[java.lang.Double] == midpointMillis
    )
  }

  it should "combine per-worker partials through merge" in {
    val agg = op(AggregationFunction.AVERAGE).getAggFunc(AttributeType.TIMESTAMP)
    val p1 = partialOf(agg, "v", AttributeType.TIMESTAMP, Seq(earlier))
    val p2 = partialOf(agg, "v", AttributeType.TIMESTAMP, Seq(later))
    assert(agg.finalAgg(agg.merge(p1, p2)).asInstanceOf[java.lang.Double] == midpointMillis)
  }

  it should "ignore null timestamps and return null when every value is null" in {
    assert(
      runSingleAttrAgg(
        AggregationFunction.AVERAGE,
        "v",
        AttributeType.TIMESTAMP,
        Seq(earlier, null, later)
      ).asInstanceOf[java.lang.Double] == midpointMillis
    )
    assert(
      runSingleAttrAgg(
        AggregationFunction.AVERAGE,
        "v",
        AttributeType.TIMESTAMP,
        Seq(null)
      ) == null
    )
  }

  "SUM aggregation merge" should "add two partials, matching a single-pass SUM" in {
    val agg = op(AggregationFunction.SUM).getAggFunc(AttributeType.LONG)
    val left = Seq[AnyRef](Long.box(1L), Long.box(2L))
    val right = Seq[AnyRef](Long.box(10L), Long.box(20L))
    val p1 = partialOf(agg, "v", AttributeType.LONG, left)
    val p2 = partialOf(agg, "v", AttributeType.LONG, right)

    assert(agg.finalAgg(agg.merge(p1, p2)) == Long.box(33L))
    assert(
      agg.finalAgg(agg.merge(p1, p2)) == runSingleAttrAgg(
        AggregationFunction.SUM,
        "v",
        AttributeType.LONG,
        left ++ right
      )
    )
    assert(agg.finalAgg(agg.merge(p1, agg.init())) == Long.box(3L))
  }

  "COUNT aggregation merge" should "add the per-worker counts" in {
    val agg = op(AggregationFunction.COUNT).getAggFunc(AttributeType.INTEGER)
    val p1 = partialOf(agg, "v", AttributeType.INTEGER, Seq(Int.box(1), null, Int.box(3)))
    val p2 = partialOf(agg, "v", AttributeType.INTEGER, Seq(Int.box(4)))

    assert(agg.finalAgg(agg.merge(p1, p2)) == Int.box(3))
    assert(agg.finalAgg(agg.merge(agg.init(), agg.init())) == Int.box(0))
  }

  "MIN and MAX aggregation merge" should "pick the smaller and larger partial respectively" in {
    val minAgg = op(AggregationFunction.MIN).getAggFunc(AttributeType.DOUBLE)
    val maxAgg = op(AggregationFunction.MAX).getAggFunc(AttributeType.DOUBLE)
    val left = Seq[AnyRef](Double.box(4.0), Double.box(9.0))
    val right = Seq[AnyRef](Double.box(-1.5), Double.box(2.0))

    val minL = partialOf(minAgg, "v", AttributeType.DOUBLE, left)
    val minR = partialOf(minAgg, "v", AttributeType.DOUBLE, right)
    assert(minAgg.finalAgg(minAgg.merge(minL, minR)) == Double.box(-1.5))
    assert(minAgg.finalAgg(minAgg.merge(minR, minL)) == Double.box(-1.5))

    val maxL = partialOf(maxAgg, "v", AttributeType.DOUBLE, left)
    val maxR = partialOf(maxAgg, "v", AttributeType.DOUBLE, right)
    assert(maxAgg.finalAgg(maxAgg.merge(maxL, maxR)) == Double.box(9.0))
    assert(maxAgg.finalAgg(maxAgg.merge(maxR, maxL)) == Double.box(9.0))
  }

  "MIN aggregation merge" should "stay neutral when one side saw no values" in {
    assertNeutralMergeWithEmptyPartial(AggregationFunction.MIN, Int.box(7))
  }

  "MAX aggregation merge" should "stay neutral when one side saw no values" in {
    assertNeutralMergeWithEmptyPartial(AggregationFunction.MAX, Int.box(7))
  }

  "CONCAT aggregation" should "swallow leading nulls but keep interior ones as empty slots" in {
    assert(
      runSingleAttrAgg(
        AggregationFunction.CONCAT,
        "v",
        AttributeType.STRING,
        Seq(null, "red", null, "blue")
      ) == "red,,blue"
    )
  }

  it should "return an empty string when every value is null" in {
    assert(
      runSingleAttrAgg(AggregationFunction.CONCAT, "v", AttributeType.STRING, Seq(null)) == ""
    )
  }

  it should "stringify non-string values it is pointed at" in {
    assert(
      runSingleAttrAgg(
        AggregationFunction.CONCAT,
        "v",
        AttributeType.INTEGER,
        Seq(Int.box(1), Int.box(2))
      ) == "1,2"
    )
  }

  "getAggregationAttribute" should "name the unknown aggregation function in its error" in {
    val ex = intercept[RuntimeException](op(null).getAggregationAttribute(AttributeType.INTEGER))
    assert(ex.getMessage == "Unknown aggregation function: null")
  }

  "getFinal" should "produce a detached copy that re-reads the result column" in {
    val original = op(AggregationFunction.MAX, attribute = "src", resultAttribute = "dst")
    val copy = original.getFinal

    assert(copy ne original)
    assert(copy.aggFunction == AggregationFunction.MAX)
    assert(copy.attribute == "dst" && copy.resultAttribute == "dst")
    assert(original.attribute == "src")
  }
}
