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

package org.apache.texera.amber.operator.sort

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.jdk.CollectionConverters.IterableHasAsJava

/**
  * Integration and internal-behavior tests for [[StableMergeSortOpExec]].
  *
  * Scope & coverage:
  *  - Single-key semantics across core types (BOOLEAN, INTEGER/LONG, DOUBLE, STRING, TIMESTAMP).
  *  - Multi-key lexicographic ordering (mixed directions/types) with null/NaN handling.
  *  - Stability guarantees (relative order preserved for equal keys) and pass-through when no keys.
  *  - Incremental “bucket stack” invariants (binary-carry sizes; no adjacent equal sizes).
  *  - Operational properties (buffering behavior, idempotent onFinish, scale sanity).
  *  - Test hooks for internal merge logic (mergeSortedBuckets, pushBucketAndCombine).
  *
  * Null policy:
  *  - Nulls are always ordered last, regardless of ASC/DESC.
  *  - NaN participates as a non-null floating value per Double comparison semantics.
  *
  * Notes:
  *  - Some tests rely on package-visible test hooks to validate internals deterministically.
  */
class StableMergeSortOpExecSpec extends AnyFlatSpec {

  // ===========================================================================
  // Helpers
  // ===========================================================================

  /** Build a Schema with (name, type) pairs, in-order. */
  private def schemaOf(attributes: (String, AttributeType)*): Schema = {
    attributes.foldLeft(Schema()) {
      case (acc, (name, attrType)) => acc.add(new Attribute(name, attrType))
    }
  }

  /**
    * Construct a Tuple for the provided schema.
    *
    * @param values map-like varargs: "colName" -> value. Must provide every column.
    * @throws NoSuchElementException if a provided key is not in the schema.
    */
  private def tupleOf(schema: Schema, values: (String, Any)*): Tuple = {
    val valueMap = values.toMap
    val builder = Tuple.builder(schema)
    schema.getAttributeNames.asJava.forEach { name =>
      builder.add(schema.getAttribute(name), valueMap(name))
    }
    builder.build()
  }

  /** Convenience builder for a single sort key with direction (ASC by default). */
  private def sortKey(
      attribute: String,
      pref: SortPreference = SortPreference.ASC
  ): SortCriteriaUnit = {
    val criteria = new SortCriteriaUnit()
    criteria.attributeName = attribute
    criteria.sortPreference = pref
    criteria
  }

  /** Convert varargs keys into the operator config buffer. */
  private def sortKeysBuffer(keys: SortCriteriaUnit*): ListBuffer[SortCriteriaUnit] =
    ListBuffer(keys: _*)

  /**
    * Run the operator on an in-memory sequence of tuples and capture all output.
    * Output is only emitted at onFinish to preserve determinism.
    */
  private def runStableMergeSort(
      schema: Schema,
      tuples: Seq[Tuple]
  )(configure: StableMergeSortOpDesc => Unit): List[Tuple] = {
    val desc = new StableMergeSortOpDesc()
    configure(desc)
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()
    tuples.foreach(t => exec.processTuple(t, 0))
    val result = exec.onFinish(0).map(_.asInstanceOf[Tuple]).toList
    exec.close()
    result
  }

  /** Internal test hook to read the current bucket sizes on the stack. */
  private def getBucketSizes(exec: StableMergeSortOpExec): List[Int] = exec.debugBucketSizes

  /** Decompose an integer into its set-bit powers of two (ascending).
    * Used to check the binary-carry invariant.
    */
  private def binaryDecomposition(number: Int): List[Int] = {
    var remaining = number
    val powers = scala.collection.mutable.ListBuffer[Int]()
    while (remaining > 0) {
      val lowestSetBit = Integer.lowestOneBit(remaining)
      powers += lowestSetBit
      remaining -= lowestSetBit
    }
    powers.toList
  }

  // ===========================================================================
  // A. Single-key semantics
  // ===========================================================================

  "StableMergeSortOpExec" should "sort integers ascending and preserve duplicate order" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = List(
      tupleOf(schema, "value" -> 3, "label" -> "a"),
      tupleOf(schema, "value" -> 1, "label" -> "first-1"),
      tupleOf(schema, "value" -> 2, "label" -> "b"),
      tupleOf(schema, "value" -> 1, "label" -> "first-2"),
      tupleOf(schema, "value" -> 3, "label" -> "c")
    )
    val result = runStableMergeSort(schema, tuples) { _.keys = sortKeysBuffer(sortKey("value")) }
    assert(result.map(_.getField[Int]("value")) == List(1, 1, 2, 3, 3))
    val labelsForOnes =
      result.filter(_.getField[Int]("value") == 1).map(_.getField[String]("label"))
    assert(labelsForOnes == List("first-1", "first-2"))
  }

  it should "sort integers descending while preserving stability" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = List(
      tupleOf(schema, "value" -> 2, "label" -> "first"),
      tupleOf(schema, "value" -> 2, "label" -> "second"),
      tupleOf(schema, "value" -> 1, "label" -> "third"),
      tupleOf(schema, "value" -> 3, "label" -> "fourth")
    )
    val result = runStableMergeSort(schema, tuples) {
      _.keys = sortKeysBuffer(sortKey("value", SortPreference.DESC))
    }
    assert(result.map(_.getField[Int]("value")) == List(3, 2, 2, 1))
    val labelsForTwos =
      result.filter(_.getField[Int]("value") == 2).map(_.getField[String]("label"))
    assert(labelsForTwos == List("first", "second"))
  }

  it should "handle string ordering (case-sensitive)" in {
    val schema = schemaOf("name" -> AttributeType.STRING)
    val tuples = List(
      tupleOf(schema, "name" -> "apple"),
      tupleOf(schema, "name" -> "Banana"),
      tupleOf(schema, "name" -> "banana"),
      tupleOf(schema, "name" -> "APPLE")
    )
    val sorted = runStableMergeSort(schema, tuples) {
      _.keys = sortKeysBuffer(sortKey("name", SortPreference.ASC))
    }
    assert(sorted.map(_.getField[String]("name")) == List("APPLE", "Banana", "apple", "banana"))
  }

  it should "order ASCII strings by Java compareTo (punctuation < digits < uppercase < lowercase)" in {
    val schema = schemaOf("str" -> AttributeType.STRING)
    val tuples = List("a", "A", "0", "~", "!").map(s => tupleOf(schema, "str" -> s))
    val result = runStableMergeSort(schema, tuples) { _.keys = sortKeysBuffer(sortKey("str")) }
    assert(result.map(_.getField[String]("str")) == List("!", "0", "A", "a", "~"))
  }

  it should "sort negatives and zeros correctly" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val tuples = List(0, -1, -10, 5, -3, 2).map(v => tupleOf(schema, "value" -> v))
    val result = runStableMergeSort(schema, tuples) { _.keys = sortKeysBuffer(sortKey("value")) }
    assert(result.map(_.getField[Int]("value")) == List(-10, -3, -1, 0, 2, 5))
  }

  it should "sort LONG values ascending" in {
    val schema = schemaOf("id" -> AttributeType.LONG)
    val tuples = List(5L, 1L, 3L, 9L, 0L).map(v => tupleOf(schema, "id" -> v))
    val result = runStableMergeSort(schema, tuples) { _.keys = sortKeysBuffer(sortKey("id")) }
    assert(result.map(_.getField[Long]("id")) == List(0L, 1L, 3L, 5L, 9L))
  }

  it should "sort TIMESTAMP ascending" in {
    val schema = schemaOf("timestamp" -> AttributeType.TIMESTAMP)
    val base = Timestamp.valueOf("2022-01-01 00:00:00")
    val tuples = List(
      new Timestamp(base.getTime + 4000),
      new Timestamp(base.getTime + 1000),
      new Timestamp(base.getTime + 3000),
      new Timestamp(base.getTime + 2000)
    ).map(ts => tupleOf(schema, "timestamp" -> ts))
    val result = runStableMergeSort(schema, tuples) {
      _.keys = sortKeysBuffer(sortKey("timestamp", SortPreference.ASC))
    }
    val times = result.map(_.getField[Timestamp]("timestamp").getTime)
    assert(times == times.sorted)
  }

  it should "sort TIMESTAMP descending" in {
    val schema = schemaOf("timestamp" -> AttributeType.TIMESTAMP)
    val base = Timestamp.valueOf("2023-01-01 00:00:00")
    val tuples = List(
      new Timestamp(base.getTime + 3000),
      base,
      new Timestamp(base.getTime + 1000),
      new Timestamp(base.getTime + 2000)
    ).map(ts => tupleOf(schema, "timestamp" -> ts))
    val result = runStableMergeSort(schema, tuples) {
      _.keys = sortKeysBuffer(sortKey("timestamp", SortPreference.DESC))
    }
    val times = result.map(_.getField[Timestamp]("timestamp").getTime)
    assert(times == times.sorted(Ordering.Long.reverse))
  }

  it should "treat numeric strings as strings (lexicographic ordering)" in {
    val schema = schemaOf("str" -> AttributeType.STRING)
    val tuples = List("2", "10", "1", "11", "20").map(s => tupleOf(schema, "str" -> s))
    val result = runStableMergeSort(schema, tuples) { _.keys = sortKeysBuffer(sortKey("str")) }
    assert(result.map(_.getField[String]("str")) == List("1", "10", "11", "2", "20"))
  }

  it should "sort BOOLEAN ascending (false < true) and descending" in {
    val schema = schemaOf("bool" -> AttributeType.BOOLEAN)
    val tuples = List(true, false, true, false).map(v => tupleOf(schema, "bool" -> v))
    val asc = runStableMergeSort(schema, tuples) {
      _.keys = sortKeysBuffer(sortKey("bool", SortPreference.ASC))
    }
    assert(asc.map(_.getField[Boolean]("bool")) == List(false, false, true, true))
    val desc = runStableMergeSort(schema, tuples) {
      _.keys = sortKeysBuffer(sortKey("bool", SortPreference.DESC))
    }
    assert(desc.map(_.getField[Boolean]("bool")) == List(true, true, false, false))
  }

  it should "sort BINARY ascending (unsigned lexicographic) incl. empty and high-bit bytes" in {
    val schema = schemaOf("bin" -> AttributeType.BINARY)

    val bytesEmpty = Array[Byte]() // []
    val bytes00 = Array(0x00.toByte) // [00]
    val bytes0000 = Array(0x00.toByte, 0x00.toByte) // [00,00]
    val bytes0001 = Array(0x00.toByte, 0x01.toByte) // [00,01]
    val bytes7F = Array(0x7f.toByte) // [7F]
    val bytes80 = Array(0x80.toByte) // [80] (-128)
    val bytesFF = Array(0xff.toByte) // [FF] (-1)

    val inputTuples = List(bytes80, bytes0000, bytesEmpty, bytesFF, bytes0001, bytes00, bytes7F)
      .map(arr => tupleOf(schema, "bin" -> arr))

    val sorted = runStableMergeSort(schema, inputTuples) { _.keys = sortKeysBuffer(sortKey("bin")) }

    val actualUnsigned = sorted.map(_.getField[Array[Byte]]("bin").toSeq.map(b => b & 0xff))
    val expectedUnsigned =
      List(bytesEmpty, bytes00, bytes0000, bytes0001, bytes7F, bytes80, bytesFF)
        .map(_.toSeq.map(b => b & 0xff))

    assert(actualUnsigned == expectedUnsigned)
  }

  // ===========================================================================
  // B. Floating-point & Null/NaN policy
  // ===========================================================================

  it should "sort DOUBLE values including -0.0, 0.0, infinities and NaN" in {
    val schema = schemaOf("x" -> AttributeType.DOUBLE)
    val tuples =
      List(Double.NaN, Double.PositiveInfinity, 1.5, -0.0, 0.0, -3.2, Double.NegativeInfinity)
        .map(v => tupleOf(schema, "x" -> v))
    val result = runStableMergeSort(schema, tuples) {
      _.keys = sortKeysBuffer(sortKey("x"))
    }
    val values = result.map(_.getField[Double]("x"))
    assert(values.head == Double.NegativeInfinity)
    assert(values(1) == -3.2)
    assert(java.lang.Double.compare(values(2), -0.0) == 0)
    assert(java.lang.Double.compare(values(3), 0.0) == 0)
    assert(values(4) == 1.5)
    assert(values(5) == Double.PositiveInfinity)
    assert(java.lang.Double.isNaN(values(6)))
  }

  it should "place NaN before null when sorting DOUBLE ascending (nulls last policy)" in {
    val schema = schemaOf("x" -> AttributeType.DOUBLE)
    val tuples = List(
      tupleOf(schema, "x" -> null),
      tupleOf(schema, "x" -> Double.NaN),
      tupleOf(schema, "x" -> Double.NegativeInfinity),
      tupleOf(schema, "x" -> 1.0),
      tupleOf(schema, "x" -> Double.PositiveInfinity),
      tupleOf(schema, "x" -> null)
    )
    val result = runStableMergeSort(schema, tuples) { _.keys = sortKeysBuffer(sortKey("x")) }
    val values = result.map(_.getField[java.lang.Double]("x"))
    assert(values.take(4).forall(_ != null)) // first 4 are non-null
    assert(values(0).isInfinite && values(0) == Double.NegativeInfinity)
    assert(values(1) == 1.0)
    assert(values(2).isInfinite && values(2) == Double.PositiveInfinity)
    assert(java.lang.Double.isNaN(values(3)))
    assert(values.drop(4).forall(_ == null))
  }

  it should "place nulls last regardless of ascending or descending" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = List(
      tupleOf(schema, "value" -> null, "label" -> "null-1"),
      tupleOf(schema, "value" -> 5, "label" -> "five"),
      tupleOf(schema, "value" -> null, "label" -> "null-2"),
      tupleOf(schema, "value" -> 3, "label" -> "three")
    )
    val asc = runStableMergeSort(schema, tuples) {
      _.keys = sortKeysBuffer(sortKey("value", SortPreference.ASC))
    }
    assert(asc.map(_.getField[String]("label")) == List("three", "five", "null-1", "null-2"))

    val desc = runStableMergeSort(schema, tuples) {
      _.keys = sortKeysBuffer(sortKey("value", SortPreference.DESC))
    }
    assert(desc.map(_.getField[String]("label")) == List("five", "three", "null-1", "null-2"))
  }

  it should "order NaN highest on secondary DESC but still place nulls last" in {
    val schema = schemaOf(
      "group" -> AttributeType.STRING,
      "score" -> AttributeType.DOUBLE,
      "label" -> AttributeType.STRING
    )
    val tuples = List(
      tupleOf(schema, "group" -> "A", "score" -> java.lang.Double.NaN, "label" -> "nan"),
      tupleOf(schema, "group" -> "A", "score" -> Double.PositiveInfinity, "label" -> "pinf"),
      tupleOf(schema, "group" -> "A", "score" -> 1.0, "label" -> "one"),
      tupleOf(schema, "group" -> "A", "score" -> 0.0, "label" -> "zero"),
      tupleOf(schema, "group" -> "A", "score" -> -1.0, "label" -> "neg"),
      tupleOf(schema, "group" -> "A", "score" -> Double.NegativeInfinity, "label" -> "ninf"),
      tupleOf(schema, "group" -> "A", "score" -> null, "label" -> "null-1"),
      tupleOf(schema, "group" -> "A", "score" -> null, "label" -> "null-2")
    )
    val result = runStableMergeSort(schema, tuples) { desc =>
      desc.keys =
        sortKeysBuffer(sortKey("group", SortPreference.ASC), sortKey("score", SortPreference.DESC))
    }
    assert(
      result.map(_.getField[String]("label")) ==
        List("nan", "pinf", "one", "zero", "neg", "ninf", "null-1", "null-2")
    )
  }

  it should "sort BINARY descending with nulls last and preserve stability for equal byte arrays" in {
    val schema = schemaOf("bin" -> AttributeType.BINARY, "id" -> AttributeType.STRING)

    val key00 = Array(0x00.toByte)
    val keyFF = Array(0xff.toByte)

    val inputTuples = List(
      tupleOf(schema, "bin" -> keyFF, "id" -> "ff-1"),
      tupleOf(schema, "bin" -> key00, "id" -> "00-1"),
      tupleOf(
        schema,
        "bin" -> key00,
        "id" -> "00-2"
      ), // equal to previous; stability should keep order
      tupleOf(schema, "bin" -> null, "id" -> "null-1")
    )

    val sorted = runStableMergeSort(schema, inputTuples) {
      _.keys = sortKeysBuffer(sortKey("bin", SortPreference.DESC))
    }

    val idsInOrder = sorted.map(_.getField[String]("id"))
    assert(idsInOrder == List("ff-1", "00-1", "00-2", "null-1"))
  }
  // ===========================================================================
  // C. Multi-key semantics (lexicographic)
  // ===========================================================================

  it should "support multi-key sorting with mixed attribute types" in {
    val schema = schemaOf(
      "dept" -> AttributeType.STRING,
      "score" -> AttributeType.DOUBLE,
      "name" -> AttributeType.STRING,
      "hired" -> AttributeType.TIMESTAMP
    )
    val base = new Timestamp(Timestamp.valueOf("2020-01-01 00:00:00").getTime)
    val tuples = List(
      tupleOf(schema, "dept" -> "Sales", "score" -> 9.5, "name" -> "Alice", "hired" -> base),
      tupleOf(
        schema,
        "dept" -> "Sales",
        "score" -> 9.5,
        "name" -> "Bob",
        "hired" -> new Timestamp(base.getTime + 1000)
      ),
      tupleOf(
        schema,
        "dept" -> "Sales",
        "score" -> 8.0,
        "name" -> "Carol",
        "hired" -> new Timestamp(base.getTime + 2000)
      ),
      tupleOf(
        schema,
        "dept" -> "Engineering",
        "score" -> 9.5,
        "name" -> "Dave",
        "hired" -> new Timestamp(base.getTime + 3000)
      ),
      tupleOf(
        schema,
        "dept" -> null,
        "score" -> 9.5,
        "name" -> "Eve",
        "hired" -> new Timestamp(base.getTime + 4000)
      )
    )
    val result = runStableMergeSort(schema, tuples) { desc =>
      desc.keys = sortKeysBuffer(
        sortKey("dept", SortPreference.ASC),
        sortKey("score", SortPreference.DESC),
        sortKey("name", SortPreference.ASC)
      )
    }
    assert(result.map(_.getField[String]("name")) == List("Dave", "Alice", "Bob", "Carol", "Eve"))
  }

  it should "handle multi-key with descending primary and ascending secondary" in {
    val schema = schemaOf(
      "major" -> AttributeType.INTEGER,
      "minor" -> AttributeType.INTEGER,
      "idx" -> AttributeType.INTEGER
    )
    val tuples = List(
      (1, 9, 0),
      (1, 1, 1),
      (2, 5, 2),
      (2, 3, 3),
      (1, 1, 4),
      (3, 0, 5),
      (3, 2, 6)
    ).map { case (ma, mi, i) => tupleOf(schema, "major" -> ma, "minor" -> mi, "idx" -> i) }
    val result = runStableMergeSort(schema, tuples) { desc =>
      desc.keys =
        sortKeysBuffer(sortKey("major", SortPreference.DESC), sortKey("minor", SortPreference.ASC))
    }
    val pairs = result.map(t => (t.getField[Int]("major"), t.getField[Int]("minor")))
    assert(pairs == List((3, 0), (3, 2), (2, 3), (2, 5), (1, 1), (1, 1), (1, 9)))
    val idxFor11 = result
      .filter(t => t.getField[Int]("major") == 1 && t.getField[Int]("minor") == 1)
      .map(_.getField[Int]("idx"))
    assert(idxFor11 == List(1, 4))
  }

  it should "use the third key as a tiebreaker (ASC, ASC, then DESC)" in {
    val schema = schemaOf(
      "keyA" -> AttributeType.INTEGER,
      "keyB" -> AttributeType.INTEGER,
      "keyC" -> AttributeType.INTEGER,
      "id" -> AttributeType.STRING
    )
    val tuples = List(
      (1, 1, 1, "x1"),
      (1, 1, 3, "x3"),
      (1, 1, 2, "x2"),
      (1, 0, 9, "y9")
    ).map {
      case (a, b, c, id) => tupleOf(schema, "keyA" -> a, "keyB" -> b, "keyC" -> c, "id" -> id)
    }
    val result = runStableMergeSort(schema, tuples) {
      _.keys =
        sortKeysBuffer(sortKey("keyA"), sortKey("keyB"), sortKey("keyC", SortPreference.DESC))
    }
    assert(result.map(_.getField[String]("id")) == List("y9", "x3", "x2", "x1"))
  }

  it should "place nulls last across multiple keys (primary ASC, secondary DESC)" in {
    val schema = schemaOf("keyA" -> AttributeType.STRING, "keyB" -> AttributeType.INTEGER)
    val tuples = List(
      ("x", 2),
      (null, 1),
      ("x", 1),
      (null, 5),
      ("a", 9),
      ("a", 2)
    ).map { case (s, i) => tupleOf(schema, "keyA" -> s, "keyB" -> i) }
    val result = runStableMergeSort(schema, tuples) { desc =>
      desc.keys =
        sortKeysBuffer(sortKey("keyA", SortPreference.ASC), sortKey("keyB", SortPreference.DESC))
    }
    val out = result.map(t => (t.getField[String]("keyA"), t.getField[Int]("keyB")))
    assert(out == List(("a", 9), ("a", 2), ("x", 2), ("x", 1), (null, 5), (null, 1)))
  }

  it should "when primary keys are both null, fall back to secondary ASC (nulls still after non-nulls)" in {
    val schema = schemaOf(
      "keyA" -> AttributeType.STRING,
      "keyB" -> AttributeType.INTEGER,
      "id" -> AttributeType.STRING
    )
    val tuples = List(
      tupleOf(schema, "keyA" -> "A", "keyB" -> 2, "id" -> "non-null-a"),
      tupleOf(schema, "keyA" -> null, "keyB" -> 5, "id" -> "null-a-5"),
      tupleOf(schema, "keyA" -> null, "keyB" -> 1, "id" -> "null-a-1"),
      tupleOf(schema, "keyA" -> "B", "keyB" -> 9, "id" -> "non-null-b")
    )
    val result = runStableMergeSort(schema, tuples) {
      _.keys = sortKeysBuffer(sortKey("keyA"), sortKey("keyB"))
    }
    assert(
      result
        .map(_.getField[String]("id")) == List("non-null-a", "non-null-b", "null-a-1", "null-a-5")
    )
  }

  it should "use INTEGER secondary key to break ties when primary BINARY keys are equal" in {
    val schema = schemaOf(
      "bin" -> AttributeType.BINARY,
      "score" -> AttributeType.INTEGER,
      "label" -> AttributeType.STRING
    )

    val key00 = Array(0x00.toByte)
    val key01 = Array(0x01.toByte)

    val inputTuples = List(
      tupleOf(schema, "bin" -> key01, "score" -> 1, "label" -> "01-score1"),
      tupleOf(schema, "bin" -> key00, "score" -> 9, "label" -> "00-score9"),
      tupleOf(schema, "bin" -> key01, "score" -> 2, "label" -> "01-score2")
    )

    val sorted = runStableMergeSort(schema, inputTuples) { desc =>
      desc.keys = sortKeysBuffer(
        sortKey("bin", SortPreference.ASC), // primary: binary ascending
        sortKey("score", SortPreference.DESC) // secondary: integer descending
      )
    }

    val labelsInOrder = sorted.map(_.getField[String]("label"))
    assert(labelsInOrder == List("00-score9", "01-score2", "01-score1"))
  }
  // ===========================================================================
  // D. Stability & operational behaviors
  // ===========================================================================

  it should "preserve original order among tuples with equal keys" in {
    val schema = schemaOf("key" -> AttributeType.INTEGER, "index" -> AttributeType.INTEGER)
    val tuples = (0 until 100).map(i => tupleOf(schema, "key" -> (i % 5), "index" -> i))
    val result = runStableMergeSort(schema, tuples) { _.keys = sortKeysBuffer(sortKey("key")) }
    val grouped = result.groupBy(_.getField[Int]("key")).values
    grouped.foreach { group =>
      val indices = group.map(_.getField[Int]("index"))
      assert(indices == indices.sorted)
    }
  }

  it should "act as a stable pass-through when keys are empty" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = List(3, 1, 4, 1, 5, 9).zipWithIndex
      .map { case (v, i) => tupleOf(schema, "value" -> v, "label" -> s"row-$i") }
    val result = runStableMergeSort(schema, tuples) { desc =>
      desc.keys = ListBuffer.empty[SortCriteriaUnit]
    }
    assert(
      result.map(_.getField[String]("label")) ==
        List("row-0", "row-1", "row-2", "row-3", "row-4", "row-5")
    )
  }

  it should "buffer tuples until onFinish is called" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val tuple = tupleOf(schema, "value" -> 2)
    val desc = new StableMergeSortOpDesc(); desc.keys = sortKeysBuffer(sortKey("value"))
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()
    val immediate = exec.processTuple(tuple, 0)
    assert(immediate.isEmpty)
    val result = exec.onFinish(0).map(_.asInstanceOf[Tuple]).toList
    assert(result.map(_.getField[Int]("value")) == List(2))
    exec.close()
  }

  it should "return empty for empty input" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val result = runStableMergeSort(schema, Seq.empty) { _.keys = sortKeysBuffer(sortKey("value")) }
    assert(result.isEmpty)
  }

  it should "handle single element input" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val result = runStableMergeSort(schema, Seq(tupleOf(schema, "value" -> 42))) {
      _.keys = sortKeysBuffer(sortKey("value"))
    }
    assert(result.map(_.getField[Int]("value")) == List(42))
  }

  it should "sort large inputs efficiently (sanity on boundaries)" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER, "label" -> AttributeType.STRING)
    val tuples = (50000 to 1 by -1).map(i => tupleOf(schema, "value" -> i, "label" -> s"row-$i"))
    val result = runStableMergeSort(schema, tuples) { _.keys = sortKeysBuffer(sortKey("value")) }
    assert(result.head.getField[Int]("value") == 1)
    assert(result(1).getField[Int]("value") == 2)
    assert(result.takeRight(2).map(_.getField[Int]("value")) == List(49999, 50000))
  }

  // ===========================================================================
  // E. Incremental bucket invariants (binary-carry & no-adjacent-equal)
  // ===========================================================================

  it should "merge incrementally: bucket sizes match binary decomposition after each push" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val desc = new StableMergeSortOpDesc(); desc.keys = sortKeysBuffer(sortKey("value"))
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val totalCount = 64
    for (index <- (totalCount - 1) to 0 by -1) {
      exec.processTuple(tupleOf(schema, "value" -> index), 0)
      val sizes = getBucketSizes(exec).sorted
      assert(sizes == binaryDecomposition(totalCount - index))
    }

    exec.close()
  }

  it should "maintain bucket-stack invariant (no adjacent equal sizes) after each insertion" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val desc = new StableMergeSortOpDesc(); desc.keys = sortKeysBuffer(sortKey("value"))
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val totalCount = 200
    val stream = (0 until totalCount by 2) ++ (1 until totalCount by 2)
    stream.foreach { index =>
      exec.processTuple(tupleOf(schema, "value" -> (totalCount - 1 - index)), 0)
      val sizes = getBucketSizes(exec)
      sizes.sliding(2).foreach { pair =>
        if (pair.length == 2) assert(pair.head != pair.last)
      }
    }

    exec.close()
  }

  it should "form expected bucket sizes at milestones (1,2,3,4,7,8,15,16)" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val desc = new StableMergeSortOpDesc(); desc.keys = sortKeysBuffer(sortKey("value"))
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc))
    exec.open()

    val inputSequence = (100 to 1 by -1).map(i => tupleOf(schema, "value" -> i))
    val milestones = Set(1, 2, 3, 4, 7, 8, 15, 16)
    var pushed = 0
    inputSequence.foreach { t =>
      exec.processTuple(t, 0); pushed += 1
      if (milestones.contains(pushed)) {
        val sizes = getBucketSizes(exec).sorted
        assert(sizes == binaryDecomposition(pushed))
      }
    }

    exec.close()
  }

  // ===========================================================================
  // F. Internal hooks — merge behavior
  // ===========================================================================

  "mergeSortedBuckets" should "be stable: left bucket wins on equal keys" in {
    val schema = schemaOf("key" -> AttributeType.INTEGER, "id" -> AttributeType.STRING)
    val desc = new StableMergeSortOpDesc(); desc.keys = sortKeysBuffer(sortKey("key"))
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc)); exec.open()

    // Seed to resolve schema/keys once.
    exec.processTuple(tupleOf(schema, "key" -> 0, "id" -> "seed"), 0)

    val left = ArrayBuffer(
      tupleOf(schema, "key" -> 1, "id" -> "L1"),
      tupleOf(schema, "key" -> 2, "id" -> "L2")
    )
    val right = ArrayBuffer(
      tupleOf(schema, "key" -> 1, "id" -> "R1"),
      tupleOf(schema, "key" -> 3, "id" -> "R3")
    )

    val merged = exec.mergeSortedBuckets(left, right)
    val ids = merged.map(_.getField[String]("id")).toList
    assert(ids == List("L1", "R1", "L2", "R3"))
    exec.close()
  }

  "mergeSortedBuckets" should "handle empty left bucket" in {
    val schema = schemaOf("key" -> AttributeType.INTEGER, "id" -> AttributeType.STRING)
    val desc = new StableMergeSortOpDesc(); desc.keys = sortKeysBuffer(sortKey("key"))
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc)); exec.open()
    exec.processTuple(tupleOf(schema, "key" -> 0, "id" -> "seed"), 0) // seed keys

    val left = ArrayBuffer.empty[Tuple]
    val right = ArrayBuffer(
      tupleOf(schema, "key" -> 1, "id" -> "r1"),
      tupleOf(schema, "key" -> 2, "id" -> "r2")
    )
    val merged = exec.mergeSortedBuckets(left, right)
    assert(merged.map(_.getField[String]("id")).toList == List("r1", "r2"))
    exec.close()
  }

  "mergeSortedBuckets" should "handle empty right bucket" in {
    val schema = schemaOf("key" -> AttributeType.INTEGER, "id" -> AttributeType.STRING)
    val desc = new StableMergeSortOpDesc(); desc.keys = sortKeysBuffer(sortKey("key"))
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc)); exec.open()
    exec.processTuple(tupleOf(schema, "key" -> 0, "id" -> "seed"), 0)

    val left = ArrayBuffer(
      tupleOf(schema, "key" -> 1, "id" -> "l1"),
      tupleOf(schema, "key" -> 2, "id" -> "l2")
    )
    val right = ArrayBuffer.empty[Tuple]
    val merged = exec.mergeSortedBuckets(left, right)
    assert(merged.map(_.getField[String]("id")).toList == List("l1", "l2"))
    exec.close()
  }

  // ===========================================================================
  // G. Internal hooks — push/finish/idempotence & schema errors
  // ===========================================================================

  "pushBucketAndCombine" should "merge two size-2 buckets into size-4 on push (with existing size-1 seed)" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val desc = new StableMergeSortOpDesc(); desc.keys = sortKeysBuffer(sortKey("value"))
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc)); exec.open()

    // seed to compile keys -> results in one size-1 bucket in the stack
    exec.processTuple(tupleOf(schema, "value" -> 0), 0)

    // two pre-sorted buckets of size 2
    val bucket1 = ArrayBuffer(tupleOf(schema, "value" -> 1), tupleOf(schema, "value" -> 3))
    val bucket2 = ArrayBuffer(tupleOf(schema, "value" -> 2), tupleOf(schema, "value" -> 4))

    exec.pushBucketAndCombine(bucket1) // sizes now [1,2]
    exec.pushBucketAndCombine(bucket2) // equal top [2,2] => merged to 4; sizes [1,4]

    val sizes = getBucketSizes(exec)
    assert(sizes == List(1, 4))
    exec.close()
  }

  it should "return the same sorted output if onFinish is called twice in a row" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val desc = new StableMergeSortOpDesc(); desc.keys = sortKeysBuffer(sortKey("value"))
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc)); exec.open()
    List(3, 1, 2).foreach(i => exec.processTuple(tupleOf(schema, "value" -> i), 0))

    val first = exec.onFinish(0).map(_.asInstanceOf[Tuple]).toList.map(_.getField[Int]("value"))
    val second = exec.onFinish(0).map(_.asInstanceOf[Tuple]).toList.map(_.getField[Int]("value"))
    assert(first == List(1, 2, 3))
    assert(second == List(1, 2, 3))
    exec.close()
  }

  it should "have processTuple always return empty iterators until finish" in {
    val schema = schemaOf("value" -> AttributeType.INTEGER)
    val desc = new StableMergeSortOpDesc(); desc.keys = sortKeysBuffer(sortKey("value"))
    val exec = new StableMergeSortOpExec(objectMapper.writeValueAsString(desc)); exec.open()
    val immediates = (10 to 1 by -1).map(i => exec.processTuple(tupleOf(schema, "value" -> i), 0))
    assert(immediates.forall(_.isEmpty))
    val out = exec.onFinish(0).map(_.asInstanceOf[Tuple]).toList.map(_.getField[Int]("value"))
    assert(out == (1 to 10).toList)
    exec.close()
  }

}
