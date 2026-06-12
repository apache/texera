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
import org.apache.texera.amber.operator.symmetricDifference.SymmetricDifferenceOpDesc

import java.nio.file.{Files, Path}

/**
  * Per-category runner for JVM-native transform operators — Path A goes
  * through [[OpExecHarness]] (drives `OpExecWithClassName` directly on the
  * JVM) and Path B through [[StandaloneRunner]] (drives
  * `generateStandaloneCode()`).
  *
  * Mirrors [[PythonTransformCategoryRunner]] one-for-one; the only
  * difference is which harness fills Path A. Reuses the same
  * [[TransformHandler]] trait so each registered op only has to describe
  * its fixture once, not twice.
  *
  * Handlers default to positional row comparison (`orderSensitive = true` on
  * the trait). Set-semantics ops (Intersect, Difference, SymmetricDifference)
  * and joins whose JVM HashSet/HashMap iteration order doesn't match the
  * pandas equivalent set `orderSensitive = false`, which makes the comparator
  * lex-sort both DataFrames before checking equality — see the trait's docs
  * for the trade-off.
  */
object JvmTransformCategoryRunner {

  private val handlersByClass: Map[Class[_ <: LogicalOp], TransformHandler] =
    Seq[TransformHandler](
      SpecializedFilterTransformHandler,
      IntersectTransformHandler,
      DifferenceTransformHandler,
      SymmetricDifferenceTransformHandler,
      HashJoinTransformHandler
    ).map(h => h.opDescClass -> h).toMap

  def canRun(opDescClass: Class[_ <: LogicalOp]): Boolean =
    handlersByClass.contains(opDescClass)

  def run(opDescClass: Class[_ <: LogicalOp]): Unit = {
    val handler = handlersByClass.getOrElse(
      opDescClass,
      throw new IllegalArgumentException(
        s"No TransformHandler registered for ${opDescClass.getSimpleName}. " +
          s"Add one to JvmTransformCategoryRunner.handlersByClass."
      )
    )

    val testRoot = Files.createTempDirectory(s"jvm-op-${opDescClass.getSimpleName}-")
    val (opDesc, inputs) = handler.fixture(testRoot)

    val actualDir = testRoot.resolve("actual")
    Files.createDirectories(actualDir)

    val pathA = OpExecHarness.execute(opDesc, inputs = inputs, outputDir = actualDir)

    // 1-based port indexing for StandaloneRunner — same translation
    // PythonTransformCategoryRunner does.
    val standaloneInputs: Map[Int, Path] =
      inputs.toSeq.sortBy(_._1.id).zipWithIndex.map {
        case ((_, path), idx) => (idx + 1) -> path
      }.toMap

    val pathB = StandaloneRunner.run(
      opDesc = opDesc,
      inputs = standaloneInputs,
      outputPortCount = 1,
      workDir = testRoot
    )

    val actual = pathA.outputs(PortIdentity(0))
    val expected = pathB.outputs(1)
    Comparator.assertEqual(actual, expected, orderSensitive = handler.orderSensitive)
  }
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
  *  the pandas `concat + duplicated(keep="first")` path — orderSensitive=false. */
object IntersectTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[IntersectOpDesc]
  override val orderSensitive: Boolean = false
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) =
    (new IntersectOpDesc(), SetOpFixture.writeLeftRight(testRoot))
}

/** Difference: `leftHashSet.diff(rightHashSet).iterator` — same hash-bucket
  *  order divergence as Intersect. */
object DifferenceTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[DifferenceOpDesc]
  override val orderSensitive: Boolean = false
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) =
    (new DifferenceOpDesc(), SetOpFixture.writeLeftRight(testRoot))
}

/** SymmetricDifference: union of the two diffs, hash-set backed on both
  *  sides. Most divergent of the set ops in practice — small fixtures can
  *  accidentally pass, larger ones fail. */
object SymmetricDifferenceTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[SymmetricDifferenceOpDesc]
  override val orderSensitive: Boolean = false
  override def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path]) =
    (new SymmetricDifferenceOpDesc(), SetOpFixture.writeLeftRight(testRoot))
}

/** HashJoin INNER on `id`. Build (port 0) and probe (port 1) intentionally
  *  arrive in different id orders so any probe-major / left-major mismatch
  *  between the JVM emit and `pd.merge` shows up — orderSensitive=false
  *  normalizes by lex-sort before compare. */
object HashJoinTransformHandler extends TransformHandler {
  override val opDescClass: Class[_ <: LogicalOp] = classOf[HashJoinOpDesc[_]]
  override val orderSensitive: Boolean = false

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
