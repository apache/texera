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

package org.apache.texera.amber.util

import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class VirtualIdentityUtilsSpec extends AnyFlatSpec with Matchers {

  // ----- createWorkerIdentity -----

  "createWorkerIdentity (raw fields)" should "format Worker:WF<id>-<op>-<layer>-<workerIdx>" in {
    val actor = VirtualIdentityUtils.createWorkerIdentity(
      WorkflowIdentity(7),
      operator = "myOp",
      layerName = "main",
      workerId = 3
    )
    actor.name shouldBe "Worker:WF7-myOp-main-3"
  }

  "createWorkerIdentity (PhysicalOpIdentity overload)" should "delegate to the same encoded format" in {
    val physicalOpId = PhysicalOpIdentity(OperatorIdentity("myOp"), "main")
    val actor = VirtualIdentityUtils.createWorkerIdentity(
      WorkflowIdentity(7),
      physicalOpId,
      workerId = 3
    )
    actor.name shouldBe "Worker:WF7-myOp-main-3"
  }

  // ----- getPhysicalOpId -----

  "getPhysicalOpId" should "extract operator id and layer name from a worker actor name" in {
    val actor = ActorVirtualIdentity("Worker:WF7-myOp-main-3")
    val opId = VirtualIdentityUtils.getPhysicalOpId(actor)
    opId.logicalOpId.id shouldBe "myOp"
    opId.layerName shouldBe "main"
  }

  it should "fall back to __DummyOperator/__DummyLayer for non-worker actor names" in {
    val controller = ActorVirtualIdentity("CONTROLLER")
    val opId = VirtualIdentityUtils.getPhysicalOpId(controller)
    opId.logicalOpId.id shouldBe "__DummyOperator"
    opId.layerName shouldBe "__DummyLayer"
  }

  it should "tolerate operator names that contain hyphens by greedy backtracking" in {
    // The operator capture group is `.+` which backtracks to leave the trailing
    // `-(\w+)-(\d+)` slots populated. A multi-hyphen operator name must still
    // round-trip without losing characters from the operator itself.
    val actor = ActorVirtualIdentity("Worker:WF1-multi-part-op-main-0")
    val opId = VirtualIdentityUtils.getPhysicalOpId(actor)
    opId.logicalOpId.id shouldBe "multi-part-op"
    opId.layerName shouldBe "main"
  }

  it should "misparse layer names that contain hyphens (current behavior)" in {
    // The layer capture group is `(\w+)`, which does not allow `-`. When the
    // real layer name contains hyphens (e.g. "1st-physical-op", as seen in
    // amber WorkerSpec), the greedy operator group eats most of the layer:
    // operator becomes "myOp-1st-physical" and layer becomes "op". This pins
    // the current bug so a future fix that broadens `workerNamePattern` to
    // accept hyphenated layers will surface here and force this spec to be
    // updated alongside the implementation.
    val actor = ActorVirtualIdentity("Worker:WF1-myOp-1st-physical-op-3")
    val opId = VirtualIdentityUtils.getPhysicalOpId(actor)
    opId.logicalOpId.id shouldBe "myOp-1st-physical"
    opId.layerName shouldBe "op"
  }

  // ----- getWorkerIndex -----

  "getWorkerIndex" should "return the trailing numeric workerId from a worker actor name" in {
    val actor = ActorVirtualIdentity("Worker:WF7-myOp-main-42")
    VirtualIdentityUtils.getWorkerIndex(actor) shouldBe 42
  }

  it should "throw MatchError on non-worker actor names (current behavior)" in {
    // getWorkerIndex pattern-matches on workerNamePattern with no fallback,
    // so passing a special ActorVirtualIdentity like CONTROLLER or SELF
    // yields scala.MatchError. Pinning this behavior here means a future
    // change that adds a fallback (or a different exception) breaks this
    // spec on purpose so the new contract is reviewed.
    val controller = ActorVirtualIdentity("CONTROLLER")
    assertThrows[scala.MatchError] {
      VirtualIdentityUtils.getWorkerIndex(controller)
    }
  }

  // ----- toShorterString -----

  "toShorterString" should "keep operator names <= 6 chars unchanged" in {
    val actor = ActorVirtualIdentity("Worker:WF1-myOp-main-0")
    VirtualIdentityUtils.toShorterString(actor) shouldBe "WF1-myOp-main-0"
  }

  it should "keep operator names of exactly 6 chars unchanged (boundary case)" in {
    // Pin the off-by-one boundary: the implementation uses `length > 6`, so a
    // six-character operator name must still pass through untouched. A
    // regression to `>= 6` would shorten "sixSix" and fail this spec.
    val actor = ActorVirtualIdentity("Worker:WF1-sixSix-main-0")
    VirtualIdentityUtils.toShorterString(actor) shouldBe "WF1-sixSix-main-0"
  }

  it should "shorten UUID-style operator names to op + last 6 chars of the postfix" in {
    // The operatorUUIDPattern is `(\w+)-(.+)-(\w+)`; the regex is greedy on the
    // middle segment, so `op` is the first \w+, and the trailing \w+ is the
    // postfix that gets `takeRight(6)`-ed.
    val actor = ActorVirtualIdentity("Worker:WF1-Filter-uuid12-abcdefghij-main-0")
    val shorter = VirtualIdentityUtils.toShorterString(actor)
    // postfix = "abcdefghij"; takeRight(6) = "efghij".
    shorter shouldBe "WF1-Filter-efghij-main-0"
  }

  it should "fall back to takeRight(6) when long operator name does not match the UUID pattern" in {
    // `nohyphens` is one \w+ token with no hyphens, so the UUID pattern can't
    // match (it requires at least two `-`s) and we hit the takeRight(6) branch.
    val actor = ActorVirtualIdentity("Worker:WF1-nohyphens-main-0")
    val shorter = VirtualIdentityUtils.toShorterString(actor)
    // takeRight(6) of "nohyphens" = "yphens"
    shorter shouldBe "WF1-yphens-main-0"
  }

  it should "return the actor name unchanged when it does not match the worker pattern" in {
    val controller = ActorVirtualIdentity("CONTROLLER")
    VirtualIdentityUtils.toShorterString(controller) shouldBe "CONTROLLER"
  }

  // ----- getFromActorIdForInputPortStorage -----

  "getFromActorIdForInputPortStorage" should "prefix MATERIALIZATION_READER_ to the storage URI plus actor name" in {
    val toWorker = ActorVirtualIdentity("Worker:WF1-myOp-main-0")
    val virtualReader = VirtualIdentityUtils.getFromActorIdForInputPortStorage(
      "iceberg:/warehouse/x",
      toWorker
    )
    virtualReader.name shouldBe "MATERIALIZATION_READER_iceberg:/warehouse/xWorker:WF1-myOp-main-0"
  }
}
