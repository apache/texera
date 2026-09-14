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

package org.apache.texera.amber.util

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp

/**
  * Foundation for the Arrow columnar wire format: a batch of tuples must survive
  * serializeTuples -> Arrow IPC bytes -> deserializeTuples unchanged.
  */
class ArrowIpcRoundTripSpec extends AnyFlatSpec {

  private val schema: Schema = Schema()
    .add(new Attribute("i", AttributeType.INTEGER))
    .add(new Attribute("l", AttributeType.LONG))
    .add(new Attribute("d", AttributeType.DOUBLE))
    .add(new Attribute("s", AttributeType.STRING))
    .add(new Attribute("b", AttributeType.BOOLEAN))
    .add(new Attribute("t", AttributeType.TIMESTAMP))

  private def tuple(i: Int): Tuple =
    Tuple
      .builder(schema)
      .addSequentially(
        Array(
          if (i % 10 == 0) null else Int.box(i),
          Long.box(i.toLong * 1000L),
          Double.box(i * 1.5),
          if (i % 7 == 0) null else s"row-$i",
          Boolean.box(i % 2 == 0),
          new Timestamp(1_600_000_000_000L + i.toLong * 86_400_000L)
        )
      )
      .build()

  "Arrow IPC" should "round-trip a batch of tuples unchanged across all types" in {
    val n = 5000
    val rows = (0 until n).map(tuple).toArray
    val bytes = ArrowUtils.serializeTuples(schema, rows)
    val back = ArrowUtils.deserializeTuples(bytes)
    assert(back.length == n)
    assert(back.sameElements(rows))
  }

  "Arrow IPC" should "round-trip an empty batch" in {
    val bytes = ArrowUtils.serializeTuples(schema, Array.empty[Tuple])
    assert(ArrowUtils.deserializeTuples(bytes).isEmpty)
  }

  // Python-passthrough equivalence: the Arrow batch the Python bridge sends via
  // the columnar passthrough (deserialize the ColumnarFrame bytes to a root) must
  // match the batch the row path builds (fromTexeraSchema + appendTexeraTuple).
  // Proven at the data level here; the Flight transport itself is unchanged.
  "Python passthrough" should "produce the same Arrow batch as the row path" in {
    val n = 3000
    val rows = (0 until n).map(tuple).toArray
    val allocator = new org.apache.arrow.memory.RootAllocator()
    try {
      // Row path: build the root the way writeArrowStream does.
      val rowRoot =
        org.apache.arrow.vector.VectorSchemaRoot.create(ArrowUtils.fromTexeraSchema(schema), allocator)
      rowRoot.allocateNew()
      rows.foreach(t => ArrowUtils.appendTexeraTuple(t, rowRoot))
      rowRoot.setRowCount(n)

      // Passthrough: the columnar bytes decoded back to tuples.
      val passthrough = ArrowUtils.deserializeTuples(ArrowUtils.serializeTuples(schema, rows))

      // Same schema, same row count, same values.
      assert(rowRoot.getSchema == ArrowUtils.fromTexeraSchema(schema))
      assert(rowRoot.getRowCount == passthrough.length)
      val rowDecoded = (0 until rowRoot.getRowCount).map(i => ArrowUtils.getTexeraTuple(i, rowRoot))
      assert(rowDecoded.sameElements(passthrough))
      rowRoot.close()
    } finally allocator.close()
  }
}
