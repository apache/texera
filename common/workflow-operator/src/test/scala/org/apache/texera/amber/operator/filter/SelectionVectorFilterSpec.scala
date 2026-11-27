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

import scala.util.Random

/**
  * The specialized filter's native-Arrow path.
  *
  * The selection vector (surviving row indices, no compaction) must name exactly
  * the rows the row path keeps, the fused filter->count must equal the row path's
  * survivor count without materializing a batch, and the compacting Emit path must
  * return the same rows. All three are checked against the row path (filterFunc) as
  * the source of truth.
  */
class SelectionVectorFilterSpec extends AnyFlatSpec {

  private def execFor(preds: (String, ComparisonType, String)*): SpecializedFilterOpExec = {
    val desc = new SpecializedFilterOpDesc()
    desc.predicates = preds.map { case (a, c, v) => new FilterPredicate(a, c, v) }.toList
    new SpecializedFilterOpExec(objectMapper.writeValueAsString(desc))
  }

  private val schema: Schema = Schema().add(new Attribute("v", AttributeType.INTEGER))

  private def batch(seed: Int, n: Int): Array[Tuple] = {
    val rng = new Random(seed)
    (0 until n).map { i =>
      val v: Any = if (i % 11 == 0) null else Int.box(rng.nextInt(100))
      Tuple.builder(schema).add(schema.getAttribute("v"), v).build()
    }.toArray
  }

  "selection vector" should "name exactly the rows the row path keeps" in {
    val rows = batch(1, 2000)
    val bytes = ArrowUtils.serializeTuples(schema, rows)
    val exec = execFor(("v", ComparisonType.GREATER_THAN, "50"))
    exec.open()

    val expectedIdx = rows.indices.filter(i => exec.filterFunc(rows(i))).toArray

    val sel = exec.columnarSelection(bytes, 0)
    assert(sel.isDefined, "expected a vectorized selection")
    assert(sel.get.toSeq == expectedIdx.toSeq, "selection indices must match row-path survivors")
    exec.close()
  }

  "fused filter -> count" should "equal the row-path count without materializing" in {
    val rows = batch(2, 3000)
    val bytes = ArrowUtils.serializeTuples(schema, rows)
    val exec = execFor(("v", ComparisonType.LESS_THAN_OR_EQUAL_TO, "40"))
    exec.open()

    val expected = rows.count(exec.filterFunc).toLong
    assert(exec.columnarMatchCount(bytes, 0).contains(expected))
    exec.close()
  }

  "the compact Emit path" should "still return the same surviving rows" in {
    val rows = batch(3, 2500)
    val bytes = ArrowUtils.serializeTuples(schema, rows)
    val exec = execFor(("v", ComparisonType.GREATER_THAN, "10"))
    exec.open()

    val expected = rows.count(exec.filterFunc)
    exec.processColumnarBatch(bytes, 0) match {
      case ColumnarResult.Emit(root) =>
        assert(root.getRowCount == expected)
        root.close()
      case other => fail(s"expected Emit, got $other")
    }
    exec.close()
  }

  "gather from a selection" should "yield the same rows as the row path" in {
    val rows = batch(4, 1500)
    val bytes = ArrowUtils.serializeTuples(schema, rows)
    val exec = execFor(("v", ComparisonType.GREATER_THAN, "70"))
    exec.open()

    val sel = exec.columnarSelection(bytes, 0).get
    val decoded = ArrowUtils.deserializeTuples(bytes)
    val gathered = sel.map(i => decoded(i).getField[Any]("v")).toSeq
    val expected = rows.filter(exec.filterFunc).map(_.getField[Any]("v")).toSeq
    assert(gathered == expected)
    exec.close()
  }
}
