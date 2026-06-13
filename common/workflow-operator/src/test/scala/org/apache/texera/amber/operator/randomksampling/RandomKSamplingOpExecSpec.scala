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

package org.apache.texera.amber.operator.randomksampling

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class RandomKSamplingOpExecSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixture builders
  // ---------------------------------------------------------------------------

  private val attr = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(attr)
  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(attr, Integer.valueOf(v)).build()

  private def descJson(percentage: Int): String = {
    val desc = new RandomKSamplingOpDesc
    desc.percentage = percentage
    objectMapper.writeValueAsString(desc)
  }

  /** Run `count` tuples through `exec` and return how many it emitted. */
  private def emittedCount(exec: RandomKSamplingOpExec, count: Int): Int =
    (1 to count).count(i => exec.processTuple(tuple(i), port = 0).nonEmpty)

  // ---------------------------------------------------------------------------
  // Boundary cases — 0% and 100%
  // ---------------------------------------------------------------------------
  //
  // The predicate is `(desc.percentage / 100.0) >= rand.nextDouble()`.
  // `Random.nextDouble()` returns a value in `[0.0, 1.0)`.
  //
  //   - At 100% (`1.0`), `1.0 >= rand.nextDouble()` always holds → accept all.
  //   - At 0%   (`0.0`), `0.0 >= rand.nextDouble()` holds iff `nextDouble()`
  //     returns `0.0`. The probability of that is 1 / 2^53 ≈ 10^-16 — for
  //     practical purposes, reject all.

  "RandomKSamplingOpExec with percentage = 100" should "accept every tuple" in {
    val exec = new RandomKSamplingOpExec(descJson(percentage = 100), idx = 0, workerCount = 7)
    assert(emittedCount(exec, 1000) == 1000)
  }

  "RandomKSamplingOpExec with percentage = 0" should "reject every tuple" in {
    // Edge case (`rand.nextDouble() == 0.0` would let one through) is
    // astronomically improbable — running 1000 draws with a fixed seed
    // either always passes or always fails. The latter is what the
    // implementation produces for percentage 0.
    val exec = new RandomKSamplingOpExec(descJson(percentage = 0), idx = 0, workerCount = 7)
    assert(emittedCount(exec, 1000) == 0)
  }

  // ---------------------------------------------------------------------------
  // Determinism — seed = workerCount, so the same (workerCount,
  // percentage, input-count) produces the same emission count across runs.
  // ---------------------------------------------------------------------------

  "RandomKSamplingOpExec with the same workerCount and percentage" should
    "produce the same emission count across two fresh instances (deterministic seed)" in {
    val a = new RandomKSamplingOpExec(descJson(percentage = 50), idx = 0, workerCount = 13)
    val b = new RandomKSamplingOpExec(descJson(percentage = 50), idx = 1, workerCount = 13)
    val countA = emittedCount(a, 200)
    val countB = emittedCount(b, 200)
    assert(countA == countB, s"deterministic seed should give equal counts, got $countA vs $countB")
  }

  it should "yield approximately the requested fraction over a large sample" in {
    // At 50% over 2000 tuples, the expected emission count is ~1000.
    // For a binomial(2000, 0.5), 3σ is ~67 — allow a ±150 band so the
    // case is well clear of stochastic flakiness while still catching
    // gross deviations (e.g. percentage being ignored).
    val exec = new RandomKSamplingOpExec(descJson(percentage = 50), idx = 0, workerCount = 1)
    val n = emittedCount(exec, 2000)
    assert(n >= 850 && n <= 1150, s"expected ~1000 emissions at 50%%, got $n")
  }

  // ---------------------------------------------------------------------------
  // Different worker seeds — different streams
  // ---------------------------------------------------------------------------

  "RandomKSamplingOpExec with different workerCount values" should
    "draw different sequences (the seed is workerCount)" in {
    // Two executors with the same percentage but different workerCount
    // should not produce IDENTICAL emission sequences over a meaningful
    // sample — the seed is workerCount, so the streams diverge.
    val a = new RandomKSamplingOpExec(descJson(percentage = 50), idx = 0, workerCount = 1)
    val b = new RandomKSamplingOpExec(descJson(percentage = 50), idx = 0, workerCount = 2)
    val emissionsA = (1 to 100).map(i => execEmit(a, i))
    val emissionsB = (1 to 100).map(i => execEmit(b, i))
    assert(
      emissionsA != emissionsB,
      "different workerCount seeds must produce different emission sequences"
    )
  }

  private def execEmit(exec: RandomKSamplingOpExec, i: Int): Boolean =
    exec.processTuple(tuple(i), port = 0).nonEmpty

  // ---------------------------------------------------------------------------
  // Descriptor parse failure
  // ---------------------------------------------------------------------------

  "RandomKSamplingOpExec construction" should
    "throw on malformed descriptor JSON" in {
    intercept[com.fasterxml.jackson.core.JsonProcessingException] {
      new RandomKSamplingOpExec("{not valid", idx = 0, workerCount = 1)
    }
  }
}
