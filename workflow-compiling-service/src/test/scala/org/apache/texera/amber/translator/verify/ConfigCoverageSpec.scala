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

import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Reports the harness verification tier per discovered [[StandaloneCodeGenerator]]
  * operator: RUNNABLE (auto) / RUNNABLE (curated) / FLAG (reason). The `info`
  * table printed by this spec is the coverage artifact shown at the handoff demo.
  *
  * Hard-asserts the must-run set: the operators that must appear as RUNNABLE for
  * the demo to be credible. Flagged operators are always reported with a reason —
  * never silently passed, and neither is a single withheld KIND of run
  * (see [[reportWithheldRuns]]).
  */
class ConfigCoverageSpec extends AnyFlatSpec with Matchers {

  // The ops the demo must show as runnable; extend as triage flips flags.
  private val mustRun = Set(
    "IntersectOpDesc",
    "DifferenceOpDesc",
    "SymmetricDifferenceOpDesc",
    "HashJoinOpDesc",
    "SpecializedFilterOpDesc",
    "SortOpDesc",
    "LimitOpDesc"
  )

  "the harness" should "classify every discovered operator into a tier and report coverage" in {
    val operators = OperatorBehaviorSpec.discoverStandaloneOperators()

    val rows = operators.map { opClass =>
      val name = opClass.getSimpleName
      val kind =
        if (classOf[SourceOperatorDescriptor].isAssignableFrom(opClass)) "source"
        else if (classOf[PythonOperatorDescriptor].isAssignableFrom(opClass)) "python-udf"
        else "jvm"
      val tier =
        if (kind == "source") {
          if (SourceCategoryRunner.canRun(opClass))
            s"RUNNABLE (${SourceCategoryRunner.tier(opClass)})"
          else s"FLAG (${SourceCategoryRunner.flagReason(opClass)})"
        } else
          TransformVerificationRunner.disposition(opClass) match {
            case TransformVerificationRunner.Runnable(t)     => s"RUNNABLE ($t)"
            case TransformVerificationRunner.Flagged(reason) => s"FLAG ($reason)"
          }
      (name, kind, tier)
    }

    val runnable = rows.count(_._3.startsWith("RUNNABLE"))
    info(s"Coverage: $runnable/${rows.size} operators runnable, ${rows.size - runnable} flagged")
    Seq("jvm", "python-udf", "source").foreach { k =>
      val of = rows.filter(_._2 == k)
      info(s"  $k: ${of.count(_._3.startsWith("RUNNABLE"))}/${of.size} runnable")
    }
    rows.sortBy { case (n, k, t) => (!t.startsWith("RUNNABLE"), k, n) }.foreach {
      case (name, kind, tier) => info(f"  $tier%-50s [$kind%-10s] $name")
    }

    reportWithheldRuns(operators)

    val failedTargets = rows.collect {
      case (name, _, tier) if mustRun.contains(name) && !tier.startsWith("RUNNABLE") =>
        s"$name → $tier"
    }
    withClue(s"must-run operators not runnable: $failedTargets") {
      failedTargets shouldBe empty
    }
  }

  /** RUNNABLE is per operator, but a runnable operator can still be missing one
    * KIND of run. Report those too, or the table reads as fuller than it is.
    *
    * Split by whether anyone should be waiting: a pending fix is a line someone
    * deletes when an issue closes, by design is an answer. Both name the operator,
    * so a family entry expands to the estimators it actually covers.
    */
  private def reportWithheldRuns(operators: Seq[Class[_ <: LogicalOp]]): Unit = {
    import TransformVerificationRunner._

    val withheld = for {
      opClass <- operators
      (kind, reason) <- withheldRunsFor(opClass)
    } yield (opClass.getSimpleName, kind, reason)

    val pending = withheld.collect { case (n, k, PendingFix(issue)) => (n, k, issue) }
    val byDesign = withheld.collect { case (n, k, ByDesign(why)) => (n, k, why) }
    info(
      s"Runs withheld: ${pending.size} pending a fix, " +
        s"${byDesign.size} not applicable by design"
    )
    pending.sorted.foreach {
      case (name, kind, issue) => info(f"  PENDING  $kind%-20s $name%-45s $issue")
    }
    byDesign.sorted.foreach {
      case (name, kind, why) => info(f"  BY-DESIGN $kind%-20s $name%-45s $why")
    }
  }
}
