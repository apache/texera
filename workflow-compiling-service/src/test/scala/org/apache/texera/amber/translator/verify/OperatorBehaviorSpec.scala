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

import com.fasterxml.jackson.annotation.JsonSubTypes
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.translator.verify.tags.IntegrationTest
import org.scalatest.ParallelTestExecution
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Auto-discovered behavioral-parity tests: for every operator registered
  * with [[LogicalOp]]'s `@JsonSubTypes` that implements
  * [[StandaloneCodeGenerator]], emit a test that runs both Path A (Texera
  * exec) and Path B (translator-generated Python via [[StandaloneRunner]])
  * and asserts their outputs are equivalent.
  *
  * [[TransformVerificationRunner]] decides how each non-source transform is
  * configured and whether it can run at all; sources go to
  * [[SourceCategoryRunner]]. An operator it cannot run is registered as an
  * ignored test carrying the reason, so the report lists every operator rather
  * than reading as though the unrunnable ones do not exist.
  *
  * No edits to this spec are needed when a new operator is added — reflection
  * discovers it automatically via `@JsonSubTypes`. The tier label appears in
  * the test name so the report shows which path exercised each operator.
  *
  * Requires Python 3 with pandas on the [[Comparator]] / [[StandaloneRunner]]
  * resolution chain (`UDF_PYTHON_PATH` env var, then `python3.12`).
  */
// Tagged @IntegrationTest: this is the only verify spec that forks a real
// Python process end-to-end, so CI routes it to the Python-provisioned
// integration job (see workflow-compiling-service/build.sbt WCS_TEST_FILTER).
@IntegrationTest
class OperatorBehaviorSpec extends AnyFlatSpec with Matchers with ParallelTestExecution {

  // Build the test list at class construction. Each branch below registers
  // one test (`in` for runnable, `ignore` for skipped) so the test report
  // shows every translator-eligible operator and why it did or didn't run.
  OperatorBehaviorSpec.discoverStandaloneOperators().foreach { opClass =>
    val name = opClass.getSimpleName

    if (!OperatorBehaviorSpec.isSelected(name)) {
      // Narrowed out by VERIFY_ONLY / VERIFY_SKIP, which only a local run sets.
      // Still registered, as an `ignore`, so the report lists every operator
      // rather than reading as though the narrowed-out ones do not exist.
      name should "NARROWED OUT — outside this run's VERIFY_ONLY / VERIFY_SKIP" ignore {}
    } else if (classOf[SourceOperatorDescriptor].isAssignableFrom(opClass)) {
      // Sources keep their handler-per-source design: each needs a real file
      // in its specific format, which a generic fixture can't supply.
      if (SourceCategoryRunner.canRun(opClass)) {
        name should "produce equivalent output in Texera and standalone Python (source)" in {
          SourceCategoryRunner.run(opClass)
        }
      } else {
        name should s"FLAGGED — ${SourceCategoryRunner.flagReason(opClass)}" ignore {}
      }
    } else {
      TransformVerificationRunner.disposition(opClass) match {
        case TransformVerificationRunner.Runnable(tier) =>
          name should s"produce equivalent output in Texera and standalone Python ($tier)" in {
            TransformVerificationRunner.run(opClass)
          }
        case TransformVerificationRunner.Flagged(reason) =>
          name should s"FLAGGED — $reason" ignore {
            // Reason is in the test name so the report carries it; the
            // coverage table in ConfigCoverageSpec aggregates these.
          }
      }
    }
  }

  // Not one test per operator like the rest of this spec: it is one assertion
  // over all of them, and it deliberately ignores the selection knobs above so a
  // VERIFY_ONLY run still cannot hide a broken splice site.
  "Generated standalone code" should "stay parseable when the column names are hostile" in {
    StandaloneEscapingCheck.run() shouldBe empty
  }
}

object OperatorBehaviorSpec {

  // Narrowing knobs for a local run, both unset by default, so the default run
  // is every operator: VERIFY_ONLY names the only ones to run, VERIFY_SKIP the
  // ones to leave out. Case-sensitive substrings against the operator's simple
  // name, comma-separated. Neither is set in CI, which therefore runs the lot.
  //
  // There is deliberately no third list withholding operators by default. What
  // stays withheld is narrower than an operator and lives where it can say why:
  // a single variant in [[TransformVerificationRunner.variantsNotRun]], or an
  // operator that cannot be run at all in its `knownIssues`, each against an
  // issue or a reason. A name here would withdraw an operator's every variant
  // and record nothing about what is wrong with it.
  private def patterns(envVar: String): Seq[String] =
    sys.env.getOrElse(envVar, "").split(",").iterator.map(_.trim).filter(_.nonEmpty).toSeq

  private lazy val onlyPatterns: Seq[String] = patterns("VERIFY_ONLY")
  private lazy val skipPatterns: Seq[String] = patterns("VERIFY_SKIP")

  /** True if `name` should run: in VERIFY_ONLY when that is set, and not in
    * VERIFY_SKIP. True for everything when neither is set.
    */
  def isSelected(name: String): Boolean = {
    val included = onlyPatterns.isEmpty || onlyPatterns.exists(name.contains)
    val excluded = skipPatterns.exists(name.contains)
    included && !excluded
  }

  /**
    * Enumerates every concrete subclass of [[LogicalOp]] declared in its
    * `@JsonSubTypes` annotation, filters to those implementing
    * [[StandaloneCodeGenerator]], and returns them sorted by simple name
    * (stable test report order).
    *
    * Uses the same registry Jackson uses to deserialize operators — no
    * separate discovery mechanism needed. Adding an operator to
    * `LogicalOp.@JsonSubTypes` makes it visible here automatically.
    */
  def discoverStandaloneOperators(): Seq[Class[_ <: LogicalOp]] = {
    val annotation = classOf[LogicalOp].getAnnotation(classOf[JsonSubTypes])
    if (annotation == null) Seq.empty
    else
      annotation
        .value()
        .toSeq
        .map(_.value())
        .filter(classOf[StandaloneCodeGenerator].isAssignableFrom)
        .map(_.asInstanceOf[Class[_ <: LogicalOp]])
        .distinct
        .sortBy(_.getSimpleName)
  }
}
