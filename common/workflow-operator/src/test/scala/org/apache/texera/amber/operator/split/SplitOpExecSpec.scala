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

package org.apache.texera.amber.operator.split

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class SplitOpExecSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixture
  // ---------------------------------------------------------------------------

  private val attr = new Attribute("v", AttributeType.INTEGER)
  private val schema: Schema = Schema().add(attr)
  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(attr, Integer.valueOf(v)).build()

  private def descJson(k: Int, random: Boolean = false, seed: Int = 1): String = {
    val d = new SplitOpDesc
    d.k = k
    d.random = random
    d.seed = seed
    objectMapper.writeValueAsString(d)
  }

  private def emittedPorts(
      exec: SplitOpExec,
      count: Int
  ): IndexedSeq[Option[PortIdentity]] = {
    (1 to count).map { i =>
      val out = exec.processTupleMultiPort(tuple(i), port = 0).toList
      assert(out.size == 1, s"expected exactly one emission per tuple, got: $out")
      out.head._2
    }
  }

  // ---------------------------------------------------------------------------
  // k = 100 — every tuple to the training port (0)
  // ---------------------------------------------------------------------------

  "SplitOpExec (k = 100)" should "emit every tuple on the training port (PortIdentity 0)" in {
    val exec = new SplitOpExec(descJson(k = 100, seed = 1))
    exec.open()
    try {
      val ports = emittedPorts(exec, 200)
      assert(ports.forall(_ == Some(PortIdentity(0))), s"expected all 0, got: ${ports.distinct}")
    } finally exec.close()
  }

  // ---------------------------------------------------------------------------
  // k = 0 — every tuple to the testing port (1)
  // ---------------------------------------------------------------------------

  "SplitOpExec (k = 0)" should "emit every tuple on the testing port (PortIdentity 1)" in {
    val exec = new SplitOpExec(descJson(k = 0, seed = 1))
    exec.open()
    try {
      val ports = emittedPorts(exec, 200)
      assert(ports.forall(_ == Some(PortIdentity(1))), s"expected all 1, got: ${ports.distinct}")
    } finally exec.close()
  }

  // ---------------------------------------------------------------------------
  // Deterministic seed
  // ---------------------------------------------------------------------------

  "SplitOpExec (deterministic seed)" should
    "produce identical port sequences across two fresh instances when seed + k match" in {
    val a = new SplitOpExec(descJson(k = 50, seed = 7))
    a.open()
    val b = new SplitOpExec(descJson(k = 50, seed = 7))
    b.open()
    try {
      val seqA = emittedPorts(a, 200)
      val seqB = emittedPorts(b, 200)
      assert(seqA == seqB)
    } finally {
      a.close()
      b.close()
    }
  }

  it should "approximate the requested ratio over a large sample (k = 50)" in {
    // Binomial(2000, 0.5) — 3σ ≈ 67; allow ±150 so the case is not flaky
    // while still catching gross deviations (e.g. seed being ignored).
    val exec = new SplitOpExec(descJson(k = 50, seed = 1))
    exec.open()
    try {
      val ports = emittedPorts(exec, 2000)
      val toTraining = ports.count(_ == Some(PortIdentity(0)))
      assert(toTraining >= 850 && toTraining <= 1150, s"expected ~1000, got $toTraining")
    } finally exec.close()
  }

  // ---------------------------------------------------------------------------
  // open / close lifecycle
  // ---------------------------------------------------------------------------

  "SplitOpExec.close" should "clear the random reference (null-out)" in {
    val exec = new SplitOpExec(descJson(k = 50, seed = 1))
    exec.open()
    assert(exec.random != null)
    exec.close()
    assert(exec.random == null)
  }

  // ---------------------------------------------------------------------------
  // processTuple (single-port overload) — unsupported
  // ---------------------------------------------------------------------------

  "SplitOpExec.processTuple" should
    "throw NotImplementedError (single-port overload is intentionally unsupported)" in {
    val exec = new SplitOpExec(descJson(k = 100))
    exec.open()
    try {
      intercept[NotImplementedError] {
        exec.processTuple(tuple(1), port = 0)
      }
    } finally exec.close()
  }

  // ---------------------------------------------------------------------------
  // Descriptor parse failure
  // ---------------------------------------------------------------------------

  "SplitOpExec construction" should
    "throw on malformed descriptor JSON" in {
    intercept[com.fasterxml.jackson.core.JsonProcessingException] {
      new SplitOpExec("{not valid")
    }
  }
}
