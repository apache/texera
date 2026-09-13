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

package org.apache.texera.amber.operator.filter

import org.apache.texera.amber.core.executor.ColumnarResult
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp
import scala.util.Random

/**
  * Lever 8.2 correctness: the vectorized batch path (processBatchMultiPort) must
  * return exactly the same surviving tuples as the row path (the stock
  * FilterPredicate), across every attribute type, every comparison operator,
  * nulls, IS NULL / IS NOT NULL, and multi-predicate OR.
  */
class VectorizedFilterCorrectnessSpec extends AnyFlatSpec {

  private val orderedOps = Seq(
    ComparisonType.GREATER_THAN,
    ComparisonType.GREATER_THAN_OR_EQUAL_TO,
    ComparisonType.LESS_THAN,
    ComparisonType.LESS_THAN_OR_EQUAL_TO,
    ComparisonType.EQUAL_TO,
    ComparisonType.NOT_EQUAL_TO
  )

  private def execFor(preds: (String, ComparisonType, String)*): SpecializedFilterOpExec = {
    val desc = new SpecializedFilterOpDesc()
    desc.predicates = preds.map { case (a, c, v) => new FilterPredicate(a, c, v) }.toList
    new SpecializedFilterOpExec(objectMapper.writeValueAsString(desc))
  }

  private def build(schema: Schema, attr: String, values: Seq[Any]): Array[Tuple] =
    values.map(v => Tuple.builder(schema).add(schema.getAttribute(attr), v).build()).toArray

  // For each operator, assert vectorized survivors == row survivors AND the fast path ran.
  private def checkAllOps(schema: Schema, rows: Array[Tuple], attr: String, value: String): Unit =
    orderedOps.foreach { op =>
      val exec = execFor((attr, op, value))
      val row = rows.filter(exec.filterFunc).toList
      val vec = exec.processBatchMultiPort(rows, 0)
      assert(vec.isDefined, s"expected vectorized path for $op on $attr")
      assert(vec.get.map(_._1).toList == row, s"mismatch: op=$op attr=$attr value=$value")
    }

  private def col(name: String, t: AttributeType) = Schema().add(new Attribute(name, t))

  "Vectorized filter" should "match the row path on DOUBLE (with nulls)" in {
    val s = col("v", AttributeType.DOUBLE)
    val rng = new Random(1)
    val rows = build(s, "v", (0 until 4000).map(i => if (i % 17 == 0) null else Double.box((rng.nextInt(100) - 10).toDouble)))
    checkAllOps(s, rows, "v", "25")
  }

  "Vectorized filter" should "match the row path on INTEGER (with nulls)" in {
    val s = col("v", AttributeType.INTEGER)
    val rng = new Random(2)
    val rows = build(s, "v", (0 until 4000).map(i => if (i % 13 == 0) null else Int.box(rng.nextInt(60))))
    checkAllOps(s, rows, "v", "30")
  }

  "Vectorized filter" should "match the row path on LONG (with nulls)" in {
    val s = col("v", AttributeType.LONG)
    val rng = new Random(3)
    val rows = build(s, "v", (0 until 4000).map(i => if (i % 11 == 0) null else Long.box(rng.nextInt(1000).toLong)))
    checkAllOps(s, rows, "v", "500")
  }

  "Vectorized filter" should "match the row path on STRING, numeric value (coercion)" in {
    val s = col("v", AttributeType.STRING)
    val rng = new Random(4)
    // mix of numeric-looking and non-numeric strings
    val rows = build(s, "v", (0 until 4000).map { i =>
      if (i % 19 == 0) null
      else if (i % 3 == 0) s"item-${rng.nextInt(50)}"
      else rng.nextInt(50).toString
    })
    checkAllOps(s, rows, "v", "25")
  }

  "Vectorized filter" should "match the row path on STRING, non-numeric value (lexicographic)" in {
    val s = col("v", AttributeType.STRING)
    val rng = new Random(5)
    val rows = build(s, "v", (0 until 3000).map(i => if (i % 23 == 0) null else s"${('A' + rng.nextInt(26)).toChar}${rng.nextInt(10)}"))
    checkAllOps(s, rows, "v", "M5")
  }

  "Vectorized filter" should "match the row path on BOOLEAN (with nulls)" in {
    val s = col("v", AttributeType.BOOLEAN)
    val rng = new Random(6)
    val rows = build(s, "v", (0 until 2000).map(i => if (i % 7 == 0) null else Boolean.box(rng.nextBoolean())))
    Seq(ComparisonType.EQUAL_TO, ComparisonType.NOT_EQUAL_TO).foreach { op =>
      val exec = execFor(("v", op, "true"))
      assert(exec.processBatchMultiPort(rows, 0).get.map(_._1).toList == rows.filter(exec.filterFunc).toList)
    }
  }

  "Vectorized filter" should "match the row path on TIMESTAMP (with nulls)" in {
    val s = col("v", AttributeType.TIMESTAMP)
    val base = 1_600_000_000_000L
    val rows = build(s, "v", (0 until 2000).map(i => if (i % 9 == 0) null else new Timestamp(base + i.toLong * 86_400_000L)))
    checkAllOps(s, rows, "v", "2020-10-01 00:00:00")
  }

  "Vectorized filter" should "match the row path for IS NULL / IS NOT NULL" in {
    val s = col("v", AttributeType.DOUBLE)
    val rows = build(s, "v", (0 until 2000).map(i => if (i % 5 == 0) null else Double.box(i.toDouble)))
    Seq(ComparisonType.IS_NULL, ComparisonType.IS_NOT_NULL).foreach { op =>
      val exec = execFor(("v", op, ""))
      assert(exec.processBatchMultiPort(rows, 0).get.map(_._1).toList == rows.filter(exec.filterFunc).toList)
    }
  }

  "Vectorized filter" should "match the row path for multi-predicate OR" in {
    val s = Schema().add(new Attribute("a", AttributeType.DOUBLE)).add(new Attribute("b", AttributeType.INTEGER))
    val rng = new Random(8)
    val rows = (0 until 3000).map { i =>
      val a: Any = if (i % 15 == 0) null else Double.box((rng.nextInt(100)).toDouble)
      val b: Any = if (i % 21 == 0) null else Int.box(rng.nextInt(100))
      Tuple.builder(s).add(s.getAttribute("a"), a).add(s.getAttribute("b"), b).build()
    }.toArray
    val exec = execFor(("a", ComparisonType.GREATER_THAN, "80"), ("b", ComparisonType.LESS_THAN, "10"))
    assert(exec.processBatchMultiPort(rows, 0).get.map(_._1).toList == rows.filter(exec.filterFunc).toList)
  }

  "Vectorized filter" should "fall back (None) when a numeric value cannot be parsed" in {
    val s = col("v", AttributeType.DOUBLE)
    val rows = build(s, "v", Seq(Double.box(1.0), Double.box(2.0)))
    val exec = execFor(("v", ComparisonType.GREATER_THAN, "not-a-number"))
    assert(exec.processBatchMultiPort(rows, 0).isEmpty)
  }

  "Native-Arrow filter" should "match the row path via processColumnarBatch (M2)" in {
    Seq(("v", AttributeType.DOUBLE, "50"), ("w", AttributeType.INTEGER, "40")).foreach {
      case (name, tpe, value) =>
        val s = col(name, tpe)
        val rng = new Random(99)
        val rows = build(
          s,
          name,
          (0 until 4000).map { i =>
            if (i % 17 == 0) null
            else if (tpe == AttributeType.DOUBLE) Double.box((rng.nextInt(100)).toDouble)
            else Int.box(rng.nextInt(100))
          }
        )
        orderedOps.foreach { op =>
          val exec = execFor((name, op, value))
          val bytes = ArrowUtils.serializeTuples(s, rows)
          val resultRoot = exec.processColumnarBatch(bytes, 0) match {
            case ColumnarResult.Emit(r) => r
            case other                  => fail(s"expected Emit, got $other")
          }
          val survivors =
            (0 until resultRoot.getRowCount).map(i => ArrowUtils.getTexeraTuple(i, resultRoot)).toList
          resultRoot.close()
          exec.close()
          val rowSurvivors = rows.filter(exec.filterFunc).toList
          assert(
            survivors.map(_.getField[Any](name)) == rowSurvivors.map(_.getField[Any](name)),
            s"native-arrow mismatch: $name $op $value"
          )
        }
    }
  }
}
