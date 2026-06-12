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

package org.apache.texera.amber.translator.verify

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.difference.DifferenceOpDesc
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.hashJoin.{HashJoinOpDesc, JoinType}
import org.apache.texera.amber.operator.intersect.IntersectOpDesc
import org.apache.texera.amber.operator.sort.{SortCriteriaUnit, SortOpDesc, SortPreference}
import org.apache.texera.amber.operator.symmetricDifference.SymmetricDifferenceOpDesc

import java.nio.file.Path

/**
  * A curated handler ships a configured OpDesc and the input fixtures it
  * needs, written once into `testRoot`. Register it in [[CuratedHandlers.all]]
  * to override the auto-config tier for that operator.
  */
trait TransformHandler {
  def opDescClass: Class[_ <: LogicalOp]
  def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path])
}

/**
  * The curated override tier of the config/fixture resolution chain: an
  * operator listed here is verified with its hand-written fixture instead of
  * the auto-generated one. This is also the seam where Xuan's curated
  * operator-field-values JSON plugs in later, as a second curated source.
  */
object CuratedHandlers {
  val all: Seq[TransformHandler] = Seq(
    SpecializedFilterTransformHandler,
    IntersectTransformHandler,
    DifferenceTransformHandler,
    SymmetricDifferenceTransformHandler,
    HashJoinTransformHandler,
    SortTransformHandler
  )

  val byClass: Map[Class[_ <: LogicalOp], TransformHandler] =
    all.map(h => h.opDescClass -> h).toMap
}

/**
  * Handler for `SpecializedFilterOpDesc`. Writes a 5-row table with an
  * integer and a string column, then filters on `age > 18 OR name == "eve"`.
  * Exercises numeric comparison, string equality (the JSON predicate `value`
  * is a string), and OR-combination of predicates — three corners of the
  * generator's coercion logic in one fixture.
  *
  * Both JVM `SpecializedFilterOpExec` and pandas boolean indexing preserve
  * input row order, so positional comparator equality holds.
  */
object SpecializedFilterTransformHandler extends TransformHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[SpecializedFilterOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("age", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING)
    )

    val rows: Seq[Tuple] = Seq(
      tupleOf(schema, 21, "alice"),
      tupleOf(schema, 17, "bob"),
      tupleOf(schema, 30, "carol"),
      tupleOf(schema, 15, "eve"),
      tupleOf(schema, 14, "dave")
    )

    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new SpecializedFilterOpDesc()
    desc.predicates = List(
      new FilterPredicate("age", ComparisonType.GREATER_THAN, "18"),
      new FilterPredicate("name", ComparisonType.EQUAL_TO, "eve")
    )

    (desc, Map(PortIdentity(0) -> inputPath))
  }

  private def tupleOf(schema: Schema, age: Int, name: String): Tuple = {
    val builder = Tuple.builder(schema)
    builder.add(schema.getAttribute("age"), Int.box(age))
    builder.add(schema.getAttribute("name"), name)
    builder.build()
  }
}

/**
  * Two-port (id INTEGER, name STRING) fixture shared by the set-op handlers.
  * 25 left rows × 31 right rows with deliberate overlap — large enough to
  * defeat the small-set / identity-hash bucket coincidences that a 3-row
  * fixture lets through, while still small enough to compare in milliseconds.
  */
private object SetOpFixture {
  val schema: Schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING)
  )

  private val names = Vector(
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
    "k", "l", "m", "n", "o", "p", "q", "r", "s", "t"
  )

  private def tup(id: Int, name: String): Tuple = {
    val b = Tuple.builder(schema)
    b.add(schema.getAttribute("id"), Int.box(id))
    b.add(schema.getAttribute("name"), name)
    b.build()
  }

  def writeLeftRight(testRoot: Path): Map[PortIdentity, Path] = {
    val leftPath = testRoot.resolve("input_port_0.jsonl")
    val rightPath = testRoot.resolve("input_port_1.jsonl")
    val left = (1 to 25).map(i => tup(i, names((i - 1) % names.size)))
    val right = (10 to 40).map(i => tup(i, names((i - 1) % names.size)))
    TupleIO.writeTuples(leftPath, left.iterator, schema)
    TupleIO.writeTuples(rightPath, right.iterator, schema)
    Map(PortIdentity(0) -> leftPath, PortIdentity(1) -> rightPath)
  }
}

/** Intersect: JVM keeps two `mutable.HashSet[Tuple]` and emits the
  *  intersection in bucket-iteration order. Row order isn't deterministic vs
  *  the pandas `concat + duplicated(keep="first")` path — order policy lives
  *  in [[TransformVerificationRunner.orderInsensitiveOps]]. */
object IntersectTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[IntersectOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) =
    (new IntersectOpDesc(), SetOpFixture.writeLeftRight(testRoot))
}

/** Difference: `leftHashSet.diff(rightHashSet).iterator` — same hash-bucket
  *  order divergence as Intersect. Order policy lives in
  *  [[TransformVerificationRunner.orderInsensitiveOps]]. */
object DifferenceTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DifferenceOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) =
    (new DifferenceOpDesc(), SetOpFixture.writeLeftRight(testRoot))
}

/** SymmetricDifference: union of the two diffs, hash-set backed on both
  *  sides. Most divergent of the set ops in practice — small fixtures can
  *  accidentally pass, larger ones fail. Order policy lives in
  *  [[TransformVerificationRunner.orderInsensitiveOps]]. */
object SymmetricDifferenceTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SymmetricDifferenceOpDesc]
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) =
    (new SymmetricDifferenceOpDesc(), SetOpFixture.writeLeftRight(testRoot))
}

/** HashJoin INNER on `id`. Build (port 0) and probe (port 1) intentionally
  *  arrive in different id orders so any probe-major / left-major mismatch
  *  between the JVM emit and `pd.merge` shows up. Order policy lives in
  *  [[TransformVerificationRunner.orderInsensitiveOps]]. */
object HashJoinTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[HashJoinOpDesc[_]]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val buildSchema = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING)
    )
    val probeSchema = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("score", AttributeType.INTEGER)
    )

    def buildTup(id: Int, name: String): Tuple = {
      val b = Tuple.builder(buildSchema)
      b.add(buildSchema.getAttribute("id"), Int.box(id))
      b.add(buildSchema.getAttribute("name"), name)
      b.build()
    }
    def probeTup(id: Int, score: Int): Tuple = {
      val b = Tuple.builder(probeSchema)
      b.add(probeSchema.getAttribute("id"), Int.box(id))
      b.add(probeSchema.getAttribute("score"), Int.box(score))
      b.build()
    }

    val buildRows = Seq(
      buildTup(3, "carol"),
      buildTup(1, "alice"),
      buildTup(5, "eve"),
      buildTup(2, "bob"),
      buildTup(4, "dave")
    )
    val probeRows = Seq(
      probeTup(1, 95),
      probeTup(2, 80),
      probeTup(3, 88),
      probeTup(4, 72),
      probeTup(5, 91)
    )
    val buildPath = testRoot.resolve("input_port_0.jsonl")
    val probePath = testRoot.resolve("input_port_1.jsonl")
    TupleIO.writeTuples(buildPath, buildRows.iterator, buildSchema)
    TupleIO.writeTuples(probePath, probeRows.iterator, probeSchema)

    val desc = new HashJoinOpDesc[Integer]()
    desc.buildAttributeName = "id"
    desc.probeAttributeName = "id"
    desc.joinType = JoinType.INNER

    (desc, Map(PortIdentity(0) -> buildPath, PortIdentity(1) -> probePath))
  }
}

/**
  * Handler for `SortOpDesc`. Writes a tiny 4-row table with one integer and
  * one string column and sorts by the integer descending — touches both
  * column types and exercises the comparator on a non-trivial reordering.
  */
object SortTransformHandler extends TransformHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[SortOpDesc]

  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) = {
    val schema = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING)
    )

    val rows: Seq[Tuple] = Seq(
      tupleOf(schema, 3, "carol"),
      tupleOf(schema, 1, "alice"),
      tupleOf(schema, 4, "dave"),
      tupleOf(schema, 2, "bob")
    )

    val inputPath = testRoot.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(inputPath, rows.iterator, schema)

    val desc = new SortOpDesc()
    val criterion = new SortCriteriaUnit()
    criterion.attributeName = "id"
    criterion.sortPreference = SortPreference.DESC
    desc.attributes = List(criterion)

    (desc, Map(PortIdentity(0) -> inputPath))
  }

  private def tupleOf(schema: Schema, id: Int, name: String): Tuple = {
    val builder = Tuple.builder(schema)
    builder.add(schema.getAttribute("id"), Int.box(id))
    builder.add(schema.getAttribute("name"), name)
    builder.build()
  }
}
