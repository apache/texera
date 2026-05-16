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

import org.apache.texera.web.model.websocket.event.OperatorAggregatedMetrics

/**
 * Single source of truth for the live per-operator profiler "heat" score used
 * by the frontend profiler heatmap. Mirrors the formulas in the TypeScript
 * `ProfilerService.computeScores` / `rawCostFor` so that if we ever need to
 * compute scores server-side (e.g., to embed into persisted runtime stats, or
 * to drive scheduler decisions), both sides agree.
 *
 * No call site in the engine yet — purely a utility for future use, per
 * profiler-m3-implementation-plan P5.
 */
object ProfilerScoring {

  sealed trait ProfilerView extends Product with Serializable
  object ProfilerView {
    case object Runtime extends ProfilerView
    case object Throughput extends ProfilerView
    case object IoImbalance extends ProfilerView

    /** Lookup by lowercase / kebab string form (matches the frontend's view ids). */
    def fromString(s: String): Option[ProfilerView] = s.toLowerCase match {
      case "runtime"      => Some(Runtime)
      case "throughput"   => Some(Throughput)
      case "io-imbalance" => Some(IoImbalance)
      case _              => None
    }
  }

  /**
   * Per-operator raw cost for the given view. NOT normalized — see [[liveScore]]
   * for the [0, 1] score derived by dividing by the peer max.
   *
   * Runtime view:        rawCost = aggregatedDataProcessingTime (ns) — higher = hotter.
   * Throughput view:     rawCost = 1 / aggregatedOutputRowCount — slower producers are hotter.
   * IoImbalance view:    rawCost = clamp(1 - out/in, 0, 1) — operators dropping most rows are hotter.
   *
   * Returns 0 for any non-finite / non-positive case (operator hasn't started,
   * counts are missing, etc.) — matches the frontend's defensive defaults.
   */
  def liveRawCost(metrics: OperatorAggregatedMetrics, view: ProfilerView): Double = {
    view match {
      case ProfilerView.Runtime =>
        val t = metrics.aggregatedDataProcessingTime.toDouble
        if (t.isFinite && t > 0) t else 0.0

      case ProfilerView.Throughput =>
        val out = metrics.aggregatedOutputRowCount
        if (out > 0) 1.0 / out.toDouble else 0.0

      case ProfilerView.IoImbalance =>
        val inp = metrics.aggregatedInputRowCount
        val out = metrics.aggregatedOutputRowCount
        if (inp <= 0) 0.0
        else clamp(1.0 - out.toDouble / inp.toDouble, 0.0, 1.0)
    }
  }

  /**
   * Normalize a single operator's raw cost against the peer maximum across the
   * workflow. Returns 0 when the peer max is 0 (the workflow hasn't produced
   * measurable work yet) and clamps the result into [0, 1].
   */
  def liveScore(rawCost: Double, peerMaxRawCost: Double): Double = {
    if (peerMaxRawCost <= 0.0 || !rawCost.isFinite || !peerMaxRawCost.isFinite) 0.0
    else clamp(rawCost / peerMaxRawCost, 0.0, 1.0)
  }

  /**
   * Convenience: compute scores for a whole map of operator id -> metrics in
   * one pass. Mirrors `ProfilerService.computeScores` exactly so both sides
   * stay apples-to-apples.
   */
  def liveScores(
      perOperator: Map[String, OperatorAggregatedMetrics],
      view: ProfilerView
  ): Map[String, Double] = {
    if (perOperator.isEmpty) return Map.empty
    val rawCosts: Map[String, Double] =
      perOperator.view.mapValues(m => liveRawCost(m, view)).toMap
    val peerMax: Double = rawCosts.values.maxOption.getOrElse(0.0)
    rawCosts.view.mapValues(rc => liveScore(rc, peerMax)).toMap
  }

  private def clamp(value: Double, min: Double, max: Double): Double =
    math.max(min, math.min(max, value))
}
