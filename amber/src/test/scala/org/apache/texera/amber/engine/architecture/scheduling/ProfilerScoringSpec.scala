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

package org.apache.texera.amber.engine.architecture.scheduling

import org.apache.texera.amber.engine.architecture.scheduling.ProfilerScoring.ProfilerView
import org.apache.texera.web.model.websocket.event.OperatorAggregatedMetrics
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Mirrors the frontend `profiler.service.spec.ts` formula assertions. If a
 * test fails here AND the corresponding frontend test still passes, the two
 * implementations have drifted — fix them together.
 */
class ProfilerScoringSpec extends AnyFlatSpec with Matchers {

  private def metrics(
      dataNs: Long = 0L,
      inRows: Long = 0L,
      outRows: Long = 0L
  ): OperatorAggregatedMetrics =
    OperatorAggregatedMetrics(
      operatorState = "Running",
      aggregatedInputRowCount = inRows,
      aggregatedInputSize = 0L,
      inputPortMetrics = Map.empty,
      aggregatedOutputRowCount = outRows,
      aggregatedOutputSize = 0L,
      outputPortMetrics = Map.empty,
      numWorkers = 1L,
      aggregatedDataProcessingTime = dataNs,
      aggregatedControlProcessingTime = 0L,
      aggregatedIdleTime = 0L
    )

  // ---- ProfilerView.fromString ---------------------------------------------

  "ProfilerView.fromString" should "round-trip every canonical id" in {
    ProfilerView.fromString("runtime") shouldBe Some(ProfilerView.Runtime)
    ProfilerView.fromString("throughput") shouldBe Some(ProfilerView.Throughput)
    ProfilerView.fromString("io-imbalance") shouldBe Some(ProfilerView.IoImbalance)
  }

  it should "be case-insensitive" in {
    ProfilerView.fromString("Runtime") shouldBe Some(ProfilerView.Runtime)
    ProfilerView.fromString("IO-IMBALANCE") shouldBe Some(ProfilerView.IoImbalance)
  }

  it should "return None for unknown views (delta is frontend-only paint, not a scoring formula)" in {
    ProfilerView.fromString("delta") shouldBe None
    ProfilerView.fromString("") shouldBe None
    ProfilerView.fromString("nope") shouldBe None
  }

  // ---- liveRawCost: runtime view -------------------------------------------

  "liveRawCost (runtime)" should "return the data-processing time when positive" in {
    ProfilerScoring.liveRawCost(metrics(dataNs = 12345L), ProfilerView.Runtime) shouldBe 12345.0
  }

  it should "return 0 for zero / negative data-processing time" in {
    ProfilerScoring.liveRawCost(metrics(dataNs = 0L), ProfilerView.Runtime) shouldBe 0.0
    ProfilerScoring.liveRawCost(metrics(dataNs = -5L), ProfilerView.Runtime) shouldBe 0.0
  }

  // ---- liveRawCost: throughput view ----------------------------------------

  "liveRawCost (throughput)" should "invert output row count — slow producers are hotter" in {
    ProfilerScoring.liveRawCost(metrics(outRows = 100L), ProfilerView.Throughput) shouldBe 0.01
    ProfilerScoring.liveRawCost(metrics(outRows = 10L), ProfilerView.Throughput) shouldBe 0.1
  }

  it should "return 0 when no output rows have been produced" in {
    ProfilerScoring.liveRawCost(metrics(outRows = 0L), ProfilerView.Throughput) shouldBe 0.0
  }

  // ---- liveRawCost: io-imbalance view --------------------------------------

  "liveRawCost (io-imbalance)" should "be 1.0 when 100% of rows are dropped" in {
    ProfilerScoring.liveRawCost(
      metrics(inRows = 1000L, outRows = 0L),
      ProfilerView.IoImbalance
    ) shouldBe 1.0
  }

  it should "be 0.0 when output >= input (passthrough or fan-out)" in {
    ProfilerScoring.liveRawCost(
      metrics(inRows = 100L, outRows = 100L),
      ProfilerView.IoImbalance
    ) shouldBe 0.0
    ProfilerScoring.liveRawCost(
      metrics(inRows = 100L, outRows = 200L),
      ProfilerView.IoImbalance
    ) shouldBe 0.0
  }

  it should "interpolate linearly for partial drops" in {
    ProfilerScoring.liveRawCost(
      metrics(inRows = 100L, outRows = 30L),
      ProfilerView.IoImbalance
    ) shouldBe (0.7 +- 1e-9)
  }

  it should "return 0 when no input rows have arrived" in {
    ProfilerScoring.liveRawCost(metrics(inRows = 0L), ProfilerView.IoImbalance) shouldBe 0.0
  }

  // ---- liveScore (normalization) -------------------------------------------

  "liveScore" should "return rawCost / peerMaxRawCost when peerMax > 0" in {
    ProfilerScoring.liveScore(50.0, 100.0) shouldBe 0.5
    ProfilerScoring.liveScore(100.0, 100.0) shouldBe 1.0
    ProfilerScoring.liveScore(0.0, 100.0) shouldBe 0.0
  }

  it should "clamp into [0, 1]" in {
    // rawCost > peerMax shouldn't happen in practice, but guard it anyway.
    ProfilerScoring.liveScore(200.0, 100.0) shouldBe 1.0
  }

  it should "return 0 when peerMax is non-positive or non-finite" in {
    ProfilerScoring.liveScore(50.0, 0.0) shouldBe 0.0
    ProfilerScoring.liveScore(50.0, -1.0) shouldBe 0.0
    ProfilerScoring.liveScore(50.0, Double.NaN) shouldBe 0.0
    ProfilerScoring.liveScore(50.0, Double.PositiveInfinity) shouldBe 0.0
  }

  it should "return 0 for non-finite rawCost (defensive)" in {
    ProfilerScoring.liveScore(Double.NaN, 100.0) shouldBe 0.0
    ProfilerScoring.liveScore(Double.PositiveInfinity, 100.0) shouldBe 0.0
  }

  // ---- liveScores: end-to-end map computation ------------------------------

  "liveScores" should "normalize across the whole workflow in one pass (runtime view)" in {
    val perOp = Map(
      "fast" -> metrics(dataNs = 200L),
      "hot"  -> metrics(dataNs = 2000L),
      "mid"  -> metrics(dataNs = 1000L)
    )
    val scores = ProfilerScoring.liveScores(perOp, ProfilerView.Runtime)
    scores("fast") shouldBe 0.1
    scores("mid") shouldBe 0.5
    scores("hot") shouldBe 1.0
  }

  it should "produce 0 for every operator when no work has been done" in {
    val perOp = Map("a" -> metrics(), "b" -> metrics())
    val scores = ProfilerScoring.liveScores(perOp, ProfilerView.Runtime)
    scores("a") shouldBe 0.0
    scores("b") shouldBe 0.0
  }

  it should "return an empty map for an empty input (no NaN from max-of-empty)" in {
    ProfilerScoring.liveScores(Map.empty, ProfilerView.Runtime) shouldBe empty
  }

  it should "score the slowest producer highest under the throughput view" in {
    val perOp = Map(
      "fast-producer" -> metrics(outRows = 1_000_000L),
      "slow-producer" -> metrics(outRows = 10L)
    )
    val scores = ProfilerScoring.liveScores(perOp, ProfilerView.Throughput)
    scores("slow-producer") shouldBe 1.0
    scores("fast-producer") should be < scores("slow-producer")
  }
}
