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

package org.apache.texera.amber.operator.trycatch

import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CatchGateOpExecSpec extends AnyFlatSpec with Matchers {

  private val schema: Schema = Schema().add("field1", AttributeType.INTEGER)
  private val tuple: Tuple = TupleLike(1).enforceSchema(schema)
  private val snapshotPort = TryCatchOpDesc.SNAPSHOT_IN.id // 0
  private val signalPort = 1

  private def mkGate(ownCone: List[String]): CatchGateOpExec = {
    val config = new CatchGateConfig()
    config.ownConeOpIds = ownCone
    new CatchGateOpExec(objectMapper.writeValueAsString(config))
  }

  private val ownError = State.errorState("myop/main", "worker-0", new RuntimeException("boom"))
  private val foreignError =
    State.errorState("upstream/main", "worker-9", new RuntimeException("outer boom"))

  private val catchPort = org.apache.texera.amber.core.workflow.PortIdentity(1)

  "CatchGate" should "drop the snapshot when the try side completed cleanly" in {
    val gate = mkGate(List("myop/main"))
    gate.processTupleMultiPort(tuple, snapshotPort) shouldBe empty
    gate.onFinishMultiPort(snapshotPort) shouldBe empty // no error rows either
  }

  it should "forward the snapshot to the catch port after an own-cone error State" in {
    val gate = mkGate(List("myop/main"))
    gate.processState(ownError, signalPort) shouldBe None // absorbed
    gate.processTupleMultiPort(tuple, snapshotPort).toList shouldBe
      List((tuple, Some(catchPort)))
  }

  it should "emit one deduplicated Error Info row per failure event" in {
    val gate = mkGate(List("myop/main"))
    gate.processState(ownError, signalPort) shouldBe None
    // same failure arriving via a second signal port (fan-out duplicate)
    gate.processState(ownError, signalPort + 1) shouldBe None
    val rows = gate.onFinishMultiPort(snapshotPort).toList
    rows should have size 1
    rows.head._2 shouldBe Some(TryCatchOpDesc.ERROR_INFO_PORT)
  }

  it should "not trigger on a foreign error (failure upstream of the frame)" in {
    val gate = mkGate(List("myop/main"))
    gate.processState(foreignError, signalPort) shouldBe None // absorbed, not ours
    gate.processTupleMultiPort(tuple, snapshotPort) shouldBe empty
    gate.onFinishMultiPort(snapshotPort) shouldBe empty // no row: not ours to report
  }

  it should "pass snapshot-lane States through (loop envelopes, foreign errors)" in {
    val gate = mkGate(List("myop/main"))
    val envelope = State(Map("some" -> "state"))
    gate.processState(envelope, snapshotPort) shouldBe Some(envelope)
    gate.processState(foreignError, snapshotPort) shouldBe Some(foreignError)
  }

  it should "absorb ordinary States on signal ports" in {
    val gate = mkGate(List("myop/main"))
    gate.processState(State(Map("some" -> "state")), signalPort) shouldBe None
  }

  it should "drop signal-lane tuples defensively" in {
    val gate = mkGate(List("myop/main"))
    gate.processState(ownError, signalPort)
    gate.processTupleMultiPort(tuple, signalPort) shouldBe empty
  }
}
