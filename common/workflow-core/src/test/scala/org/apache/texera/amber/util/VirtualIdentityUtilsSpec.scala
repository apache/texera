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

  // ----- getWorkerIndex -----

  "getWorkerIndex" should "return the trailing numeric workerId from a worker actor name" in {
    val actor = ActorVirtualIdentity("Worker:WF7-myOp-main-42")
    VirtualIdentityUtils.getWorkerIndex(actor) shouldBe 42
  }

  // Intentionally not covered: actor names that do not match workerNamePattern
  // make getWorkerIndex throw scala.MatchError because the method has no
  // fallback case. See the "Potential bug" note in the PR description.

  // ----- toShorterString -----

  "toShorterString" should "keep operator names <= 6 chars unchanged" in {
    val actor = ActorVirtualIdentity("Worker:WF1-myOp-main-0")
    VirtualIdentityUtils.toShorterString(actor) shouldBe "WF1-myOp-main-0"
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
