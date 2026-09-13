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

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{Float8Vector, VectorSchemaRoot}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, ColumnarBatch, Schema, Tuple}
import org.apache.texera.amber.util.ArrowUtils
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Spike for the columnar direction: how fast does `l_quantity > 25` run over 6M
  * rows when the data arrives ROW-oriented vs COLUMNAR (Apache Arrow)?
  *   ROW   : stock FilterPredicate per Tuple.
  *   T-COL : ColumnarBatch extracts the column from the Tuples, then a tight loop
  *           (today's vectorized filter -- still pays the extraction).
  *   ARROW : the data already lives in an Arrow Float8Vector (what a columnar
  *           wire format would give), filtered by a tight loop, no extraction.
  * The ARROW number is the end-to-end ceiling a columnar data flow unlocks.
  * Prints ARROWBENCH lines. All three produce the same survivor count.
  */
class ArrowColumnarFilterBenchSpec extends AnyFlatSpec {

  private val n = sys.env.getOrElse("ARROWBENCH_N", "6000000").toInt
  private val warmups = sys.env.getOrElse("ARROWBENCH_WARMUPS", "3").toInt
  private val runs = sys.env.getOrElse("ARROWBENCH_RUNS", "5").toInt
  private val threshold = 25.0

  private val schema: Schema = Schema().add(new Attribute("l_quantity", AttributeType.DOUBLE))
  private val attr = schema.getAttribute("l_quantity")

  private val rows: Array[Tuple] = {
    val a = new Array[Tuple](n)
    var i = 0
    while (i < n) { a(i) = Tuple.builder(schema).add(attr, Double.box((i % 50) + 1.0)).build(); i += 1 }
    a
  }

  // Fill an Arrow columnar batch once (the cost a columnar SOURCE would absorb).
  private val allocator = new RootAllocator()
  private val root: VectorSchemaRoot = {
    val r = VectorSchemaRoot.create(ArrowUtils.fromTexeraSchema(schema), allocator)
    var i = 0
    while (i < n) { ArrowUtils.setTexeraTuple(rows(i), i, r); i += 1 }
    r.setRowCount(n)
    r
  }
  private val qty: Float8Vector = root.getVector("l_quantity").asInstanceOf[Float8Vector]

  private def rowPath(): Int = {
    val pred = new FilterPredicate("l_quantity", ComparisonType.GREATER_THAN, threshold.toString)
    var c = 0; var i = 0
    while (i < n) { if (pred.evaluate(rows(i))) c += 1; i += 1 }
    c
  }

  private def tupleColPath(): Int = {
    val (vals, isNull) = new ColumnarBatch(rows).doubleColumn("l_quantity")
    var c = 0; var i = 0
    while (i < n) { if (!isNull(i) && vals(i) > threshold) c += 1; i += 1 }
    c
  }

  private def arrowColPath(): Int = {
    var c = 0; var i = 0
    while (i < n) { if (!qty.isNull(i) && qty.get(i) > threshold) c += 1; i += 1 }
    c
  }

  private def time(name: String, fn: () => Int): (Double, Int) = {
    var last = 0
    for (_ <- 0 until warmups) last = fn()
    var best = Double.MaxValue
    for (_ <- 0 until runs) {
      val t = System.nanoTime(); last = fn(); best = math.min(best, (System.nanoTime() - t) / 1e6)
    }
    println(f"ARROWBENCH $name: best=${best}%.1fms (n=$n, survivors=$last)")
    (best, last)
  }

  "Filter over 6M rows" should "compare row vs Tuple-columnar vs Arrow-columnar" in {
    val (rowMs, rc) = time("ROW  ", () => rowPath())
    val (tMs, tc) = time("T-COL", () => tupleColPath())
    val (aMs, ac) = time("ARROW", () => arrowColPath())
    assert(rc == tc && tc == ac) // identical results
    println(f"ARROWBENCH SPEEDUP  row/T-COL=${rowMs / tMs}%.1fx  row/ARROW=${rowMs / aMs}%.1fx  T-COL/ARROW=${tMs / aMs}%.1fx")
    root.close(); allocator.close()
  }
}
