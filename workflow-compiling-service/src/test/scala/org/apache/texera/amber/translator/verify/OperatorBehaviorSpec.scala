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
import org.apache.texera.amber.operator.{
  LogicalOp,
  PythonOperatorDescriptor,
  StandaloneCodeGenerator
}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Auto-discovered behavioral-parity tests: for every operator registered
  * with [[LogicalOp]]'s `@JsonSubTypes` that implements
  * [[StandaloneCodeGenerator]], emit a test that runs both Path A (Texera
  * JVM exec via [[OpExecHarness]]) and Path B (translator-generated Python
  * via [[StandaloneRunner]]) and asserts their outputs are equivalent.
  *
  * The point: adding a new operator that mixes in
  * [[StandaloneCodeGenerator]] should yield free behavioral coverage as
  * soon as it's registered in Texera. No edits to this spec required —
  * the new operator gets enumerated by reflection, dispatched to its
  * category's runner, and tested with that category's fixture defaults.
  *
  * Operators that can't be tested today are marked `ignore` (visible in
  * the test report, don't block CI) with a one-line reason:
  *   - PythonOperatorDescriptor subclasses (Sort, SpecializedFilter,
  *     BarChart, …) — [[OpExecHarness]] only drives JVM execs.
  *   - Operators outside a supported category — no category runner yet.
  *   - Source operators without a registered [[SourceHandler]] —
  *     someone needs to add one to [[SourceCategoryRunner]].
  *
  * Requires Python 3 with pandas on the [[Comparator]] / [[StandaloneRunner]]
  * resolution chain (`UDF_PYTHON_PATH` env var, then `python3.12`).
  */
class OperatorBehaviorSpec extends AnyFlatSpec with Matchers {

  // Build the test list at class construction. Each branch below registers
  // one test (`in` for runnable, `ignore` for skipped) so the test report
  // shows every translator-eligible operator and why it did or didn't run.
  OperatorBehaviorSpec.discoverStandaloneOperators().foreach { opClass =>
    val name = opClass.getSimpleName

    if (classOf[PythonOperatorDescriptor].isAssignableFrom(opClass)) {
      // Python-native ops are driven by PyOpExecHarness (via the
      // py_op_driver subprocess) instead of OpExecHarness. Per-category
      // dispatch mirrors the JVM side: sources would go through a future
      // Python source runner; everything else routes to the transform
      // runner if a handler exists for it.
      if (PythonTransformCategoryRunner.canRun(opClass)) {
        name should "produce equivalent output in Texera and standalone Python (python-native transform)" in {
          PythonTransformCategoryRunner.run(opClass)
        }
      } else {
        name should "be verified once a TransformHandler is registered" ignore {
          // To enable: add a TransformHandler in PythonTransformCategoryRunner.
        }
      }
    } else if (classOf[SourceOperatorDescriptor].isAssignableFrom(opClass)) {
      if (SourceCategoryRunner.canRun(opClass)) {
        name should "produce equivalent output in Texera and standalone Python (source)" in {
          SourceCategoryRunner.run(opClass)
        }
      } else {
        name should "be verified once a SourceHandler is registered" ignore {
          // To enable: add a SourceHandler in SourceCategoryRunner.
        }
      }
    } else if (JvmTransformCategoryRunner.canRun(opClass)) {
      // JVM-native transforms: OpExecHarness on Path A, StandaloneRunner on
      // Path B. Add the operator to JvmTransformCategoryRunner.handlersByClass
      // (along with its fixture) to flip the test from ignored to active.
      name should "produce equivalent output in Texera and standalone Python (jvm-native transform)" in {
        JvmTransformCategoryRunner.run(opClass)
      }
    } else {
      name should "be verified once a TransformHandler is registered" ignore {
        // JVM-native transform / join / set-op without a registered handler.
        // To enable: add a TransformHandler in JvmTransformCategoryRunner.
      }
    }
  }
}

object OperatorBehaviorSpec {

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
        .sortBy(_.getSimpleName)
  }
}
