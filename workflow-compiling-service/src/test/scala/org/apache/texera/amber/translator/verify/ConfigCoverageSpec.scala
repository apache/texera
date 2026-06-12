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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Measures how far the reflective [[ConfigGenerator]] gets on its own: for
  * every operator registered with [[StandaloneCodeGenerator]], try to produce a
  * valid config from metadata alone and tally success vs. the reason it can't.
  *
  * Prints a coverage table (the real number behind the "~70-75% auto-coverable"
  * estimate) and asserts the in-memory JVM-exec operators we target first all
  * configure cleanly. Operators it can't fill are reported, never silently
  * passed — that list is exactly what the curated-override layer must cover.
  */
class ConfigCoverageSpec extends AnyFlatSpec with Matchers {

  private val schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING),
    new Attribute("score", AttributeType.DOUBLE)
  )
  // Supply two ports; single-input operators just ignore port 1.
  private val schemas = Map(0 -> schema, 1 -> schema)

  // The JVM-exec, in-memory operators the harness targets first — these must
  // auto-configure for the prototype to be useful.
  private val mustCover = Set(
    "UnionOpDesc",
    "IntersectOpDesc",
    "DifferenceOpDesc",
    "SymmetricDifferenceOpDesc",
    "HashJoinOpDesc",
    "IntervalJoinOpDesc",
    "SpecializedFilterOpDesc"
  )

  "ConfigGenerator" should "auto-configure the targeted JVM-exec operators and report overall coverage" in {
    val operators = OperatorBehaviorSpec.discoverStandaloneOperators()

    val rows = operators.map { opClass =>
      val name = opClass.getSimpleName
      val kind =
        if (classOf[PythonOperatorDescriptor].isAssignableFrom(opClass)) "python-udf"
        else if (classOf[SourceOperatorDescriptor].isAssignableFrom(opClass)) "source"
        else "jvm"
      val outcome = ConfigGenerator.generate(opClass, schemas)
      (name, kind, outcome)
    }

    // Print a readable coverage table.
    val ok = rows.count(_._3.isRight)
    info(s"Config coverage: $ok/${rows.size} operators auto-configured")
    rows.sortBy { case (n, k, r) => (r.isRight, k, n) }.foreach {
      case (name, kind, Right(_))     => info(f"  OK    [$kind%-10s] $name")
      case (name, kind, Left(reason)) => info(f"  FLAG  [$kind%-10s] $name — $reason")
    }

    val jvm = rows.filter(_._2 == "jvm")
    val jvmOk = jvm.count(_._3.isRight)
    info(s"JVM-exec coverage: $jvmOk/${jvm.size}")

    // Hard assertion: the operators we target first must all configure.
    val failedTargets = rows.collect {
      case (name, _, Left(reason)) if mustCover.contains(name) => s"$name ($reason)"
    }
    withClue(s"targeted operators that failed to auto-configure: $failedTargets") {
      failedTargets shouldBe empty
    }
  }
}
