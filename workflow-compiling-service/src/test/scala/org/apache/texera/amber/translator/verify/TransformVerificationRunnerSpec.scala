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

import org.apache.texera.amber.operator.intersect.IntersectOpDesc
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.amber.operator.visualization.barChart.BarChartOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformVerificationRunnerSpec extends AnyFlatSpec with Matchers {
  import TransformVerificationRunner._

  "disposition" should "flag knownIssues operators with the triage reason" in {
    disposition(classOf[UnionOpDesc]) match {
      case Flagged(reason) => reason should include("known issue")
      case other           => fail(s"expected Flagged, got $other")
    }
  }

  it should "flag visualization operators (no DataFrame to compare)" in {
    disposition(classOf[BarChartOpDesc]) match {
      case Flagged(reason) => reason should include("visualization")
      case other           => fail(s"expected Flagged, got $other")
    }
  }

  it should "route operators with a curated handler to the curated tier" in {
    disposition(classOf[IntersectOpDesc]) shouldBe Runnable("curated")
  }

  it should "route auto-configurable operators to the auto tier" in {
    disposition(classOf[LimitOpDesc]) shouldBe Runnable("auto")
  }

  // End-to-end smoke of the curated path: Intersect is fast, JVM-native, and
  // exercises two input ports + the order-insensitive comparator branch.
  "run" should "verify IntersectOpDesc end-to-end via the curated tier" in {
    TransformVerificationRunner.run(classOf[IntersectOpDesc])
  }

  // End-to-end smoke of the auto path: Limit is single-input single-output
  // and its config is fully derivable.
  it should "verify LimitOpDesc end-to-end via the auto tier" in {
    TransformVerificationRunner.run(classOf[LimitOpDesc])
  }
}
