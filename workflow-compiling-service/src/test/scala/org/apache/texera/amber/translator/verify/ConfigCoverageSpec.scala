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
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Holds the floor under what the harness can run.
  *
  * Every operator carrying standalone code is classified runnable or flagged, and
  * a flag is how an operator stops being checked at all. For most that is a state
  * someone is working through. For the ones below it would be a regression: they
  * are the plainest operators the harness has, they take the shared table as it
  * is, and nothing about them is hard to run. An operator arriving here means
  * something upstream of the disposition broke rather than that this operator
  * became difficult.
  */
class ConfigCoverageSpec extends AnyFlatSpec with Matchers {

  private val mustRun = Set(
    "IntersectOpDesc",
    "DifferenceOpDesc",
    "SymmetricDifferenceOpDesc",
    "HashJoinOpDesc",
    "SpecializedFilterOpDesc",
    "SortOpDesc",
    "LimitOpDesc"
  )

  "the harness" should "keep runnable every operator it must be able to run" in {
    val flagged = OperatorBehaviorSpec
      .discoverStandaloneOperators()
      .filter(opClass => mustRun.contains(opClass.getSimpleName))
      .filterNot(runnable)
      .map(_.getSimpleName)
    withClue(s"must-run operators no longer runnable: $flagged") {
      flagged shouldBe empty
    }
  }

  private def runnable(opClass: Class[_ <: LogicalOp]): Boolean =
    if (classOf[SourceOperatorDescriptor].isAssignableFrom(opClass))
      SourceCategoryRunner.canRun(opClass)
    else
      TransformVerificationRunner
        .disposition(opClass)
        .isInstanceOf[TransformVerificationRunner.Runnable]
}
