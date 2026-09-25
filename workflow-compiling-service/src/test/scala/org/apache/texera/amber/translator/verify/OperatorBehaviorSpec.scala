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
  * A parity test per operator: every one registered in [[LogicalOp]]'s
  * `@JsonSubTypes` that implements [[StandaloneCodeGenerator]] runs through the
  * Texera executor and through its generated Python, and the two outputs are
  * compared. Adding an operator needs no edit here.
  *
  * [[TransformVerificationRunner]] configures each non-source transform and
  * decides whether it can run; sources go to [[SourceCategoryRunner]]. Every
  * test's NAME carries its verdict, the tier that ran it or the reason it could
  * not, so the report lists every operator and says what it did not check.
  *
  * Requires Python 3 with pandas on the [[Comparator]] / [[StandaloneRunner]]
  * resolution chain (`UDF_PYTHON_PATH` env var, then `python3.12`).
  */
// Tagged @IntegrationTest: this is the only verify spec that forks a real
// Python process end-to-end, so CI routes it to the Python-provisioned
// integration job (see workflow-compiling-service/build.sbt WCS_TEST_FILTER).
@IntegrationTest
class OperatorBehaviorSpec extends AnyFlatSpec with Matchers with ParallelTestExecution {

  // The test list is built at class construction, one test per operator.
  OperatorBehaviorSpec.discoverStandaloneOperators().foreach { opClass =>
    val name = opClass.getSimpleName

    if (!OperatorBehaviorSpec.isSelected(name)) {
      // Only a local run sets those, and CI therefore runs the lot.
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
          // ConfigCoverageSpec aggregates these into its table.
          name should s"FLAGGED — $reason" ignore {}
      }
    }
  }

  // Not one test per operator like the rest of this spec: it is one assertion
  // over all of them, and it deliberately ignores the selection knobs above so a
  // VERIFY_ONLY run still cannot hide a broken splice site.
  "Generated standalone code" should "stay parseable when the column names are hostile" in {
    StandaloneEscapingCheck.run() shouldBe empty
  }

  // Also one assertion over all of them: a workflow that draws two charts from
  // one upstream hands both the same variable, and only a plan with a branch
  // ever notices an operator writing to it.
  it should "leave the frame it was handed alone" in {
    StandaloneInputCheck.run() shouldBe empty
  }
}

object OperatorBehaviorSpec {

  // Case-sensitive substrings of the operator's simple name, comma-separated.
  //
  // There is deliberately no third list withholding operators by default: a
  // name here would withdraw an operator's every variant and record nothing
  // about what is wrong with it. Withholding lives where it can say why, in
  // [[TransformVerificationRunner]]'s `variantsNotRun` or its `knownIssues`.
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
