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
import org.apache.texera.amber.operator.sort.{SortCriteriaUnit, SortOpDesc, SortPreference}

import java.nio.file.{Files, Path}

/**
  * Per-category runner for Python-native transform operators — Path A goes
  * through [[PyOpExecHarness]] (drives `generatePythonCode()` via the
  * py_op_driver) and Path B through [[StandaloneRunner]] (drives
  * `generateStandaloneCode()`). Both outputs land as JSONL with sidecar
  * schemas; [[Comparator]] diffs them with pandas.
  *
  * Same registry-of-handlers shape as [[SourceCategoryRunner]] so the
  * dispatch in [[OperatorBehaviorSpec]] stays uniform: add an op + its
  * handler here and the spec picks it up via reflection.
  *
  * Each [[TransformHandler]] does two things:
  *   1. Writes an input fixture (JSONL + sidecar) under `testRoot` for every
  *      external input port the operator declares.
  *   2. Returns a configured OpDesc whose semantics match those input ports.
  */
object PythonTransformCategoryRunner {

  private val handlersByClass: Map[Class[_ <: LogicalOp], TransformHandler] =
    Seq[TransformHandler](SortTransformHandler).map(h => h.opDescClass -> h).toMap

  def canRun(opDescClass: Class[_ <: LogicalOp]): Boolean =
    handlersByClass.contains(opDescClass)

  def run(opDescClass: Class[_ <: LogicalOp]): Unit = {
    val handler = handlersByClass.getOrElse(
      opDescClass,
      throw new IllegalArgumentException(
        s"No TransformHandler registered for ${opDescClass.getSimpleName}. " +
          s"Add one to PythonTransformCategoryRunner.handlersByClass."
      )
    )

    val testRoot = Files.createTempDirectory(s"py-op-${opDescClass.getSimpleName}-")
    val (opDesc, inputs) = handler.fixture(testRoot)

    val actualDir = testRoot.resolve("actual")
    Files.createDirectories(actualDir)

    val pathA = PyOpExecHarness.execute(opDesc, inputs = inputs, outputDir = actualDir)

    // StandaloneRunner keys inputs by 1-based port index (matches the
    // `inNdf` placeholder convention the translator uses). PortIdentity → int
    // mapping is well-defined here because we go through the same OpDesc.
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
  * A transform handler ships an OpDesc and the input fixtures it needs.
  * The harness on either side reads the input files; the handler only has to
  * write them once into `testRoot`.
  */
trait TransformHandler {
  def opDescClass: Class[_ <: LogicalOp]

  /**
    * Materialize input JSONL fixtures inside `testRoot` and return the
    * OpDesc paired with a map from external input port → fixture path.
    */
  def fixture(testRoot: Path): (LogicalOp, Map[PortIdentity, Path])

  /**
    * `true` (default) → comparator matches rows positionally after
    * `reset_index(drop=True)`. Use this for ops whose JVM exec preserves the
    * same row order pandas produces (per-row filters, sorts, etc.).
    *
    * `false` → comparator lex-sorts both DataFrames by all columns before
    * comparing. Required for set-semantics ops (Intersect, Difference,
    * SymmetricDifference) and joins where JVM `HashSet` / `HashMap` iteration
    * order doesn't match the pandas `concat + drop_duplicates` / `pd.merge`
    * row sequence. Trade-off: weakens the parity check to set-equality, so
    * the per-row sequence semantics aren't tested anymore — only choose
    * `false` when sequence equality is genuinely unachievable.
    */
  def orderSensitive: Boolean = true
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
