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
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}

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
  * Operators registered here MUST produce row-order-deterministic output
  * (Path A and Path B emit the same rows in the same order) — the
  * comparator matches rows positionally after `reset_index(drop=True)`.
  * Order-divergent ops (set ops where the underlying impl returns a
  * different stream order than pandas, hash-partitioned joins, etc.) need
  * a sort-normalize step in the runner before they can join; not added
  * here until at least one such operator is on this branch.
  */
object JvmTransformCategoryRunner {

  private val handlersByClass: Map[Class[_ <: LogicalOp], TransformHandler] =
    Seq[TransformHandler](SpecializedFilterTransformHandler)
      .map(h => h.opDescClass -> h)
      .toMap

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
    Comparator.assertEqual(actual, expected)
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
