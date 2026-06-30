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

package org.apache.texera.amber.core.workflow.cache

import org.apache.texera.amber.core.executor.{OpExecInitInfo, OpExecWithCode}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  InputPort,
  OutputPort,
  PhysicalLink,
  PhysicalOp,
  PhysicalPlan,
  PortIdentity
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Unit tests for [[CacheKeyUtil]]. These live in the workflow-core module, next
  * to the code under test, and build a `PhysicalPlan` directly (no engine test
  * helpers), so the cache-key logic is exercised and covered in its own module.
  */
class CacheKeyUtilSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(0L)
  private val executionId = ExecutionIdentity(0L)

  private def opId(name: String): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(name), "main")

  private def physicalOp(name: String): PhysicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(opId(name), workflowId, executionId, OpExecInitInfo.Empty)
      .withInputPorts(List(InputPort(PortIdentity(0))))
      .withOutputPorts(List(OutputPort(PortIdentity(0))))

  private def link(from: String, to: String): PhysicalLink =
    PhysicalLink(opId(from), PortIdentity(0), opId(to), PortIdentity(0))

  private def outputPort(name: String): GlobalPortIdentity =
    GlobalPortIdentity(opId(name), PortIdentity(0), input = false)

  /** The cache key of the named operator's output port, computed from its upstream sub-DAG. */
  private def keyOf(plan: PhysicalPlan, name: String): StorageCacheKey =
    CacheKeyUtil.computeCacheKey(plan.getTransitiveUpstreamSubPlan(opId(name)), outputPort(name))

  /** a -> b -> c */
  private def linearPlan(): PhysicalPlan =
    PhysicalPlan(
      Set(physicalOp("a"), physicalOp("b"), physicalOp("c")),
      Set(link("a", "b"), link("b", "c"))
    )

  "CacheKeyUtil.computeCacheKey" should "be stable for the same sub-DAG and port" in {
    val plan = linearPlan()
    val k1 = keyOf(plan, "b")
    val k2 = keyOf(plan, "b")
    k1.hash shouldEqual k2.hash
    k1.json shouldEqual k2.json
    k1.hash should have length 64
  }

  it should "produce different keys for different ports in the same plan" in {
    val plan = linearPlan()
    keyOf(plan, "b").hash should not equal keyOf(plan, "c").hash
  }

  it should "include only upstream operators, not downstream ones" in {
    val plan = linearPlan()
    val key = keyOf(plan, "b")
    key.json should include(opId("a").toString)
    key.json should not include opId("c").toString
  }

  it should "be stable for a source operator with no upstream" in {
    val plan = linearPlan()
    keyOf(plan, "a").hash shouldEqual keyOf(plan, "a").hash
  }

  it should "change when the upstream structure changes" in {
    val base = linearPlan()
    // b gains a second upstream (x) on a new input port: the upstream sub-DAG of
    // b's output port is now different, so the key must differ.
    val b2 = physicalOp("b")
      .withInputPorts(List(InputPort(PortIdentity(0)), InputPort(PortIdentity(1))))
    val widened = PhysicalPlan(
      Set(physicalOp("a"), physicalOp("x"), b2, physicalOp("c")),
      Set(
        link("a", "b"),
        PhysicalLink(opId("x"), PortIdentity(0), opId("b"), PortIdentity(1)),
        link("b", "c")
      )
    )
    keyOf(base, "b").hash should not equal keyOf(widened, "b").hash
  }

  it should "change the cache key when an upstream operator's exec info changes" in {
    def planWith(code: String): PhysicalPlan =
      PhysicalPlan(
        Set(
          PhysicalOp
            .oneToOnePhysicalOp(opId("a"), workflowId, executionId, OpExecWithCode(code, "python"))
            .withInputPorts(List(InputPort(PortIdentity(0))))
            .withOutputPorts(List(OutputPort(PortIdentity(0))))
        ),
        Set.empty
      )
    keyOf(planWith("def f(t): return t"), "a").hash should not equal
      keyOf(planWith("def f(t): return t + 1"), "a").hash
  }

  it should "ignore output-port attributes that do not change the result (blocking, mode, reuseStorage)" in {
    def planWith(out: OutputPort): PhysicalPlan =
      PhysicalPlan(
        Set(
          PhysicalOp
            .oneToOnePhysicalOp(opId("a"), workflowId, executionId, OpExecInitInfo.Empty)
            .withInputPorts(List(InputPort(PortIdentity(0))))
            .withOutputPorts(List(out))
        ),
        Set.empty
      )
    // blocking (scheduling), reuseStorage (storage), and mode (how the stored result is
    // presented to the UI) do not change the materialized data, so they are intentionally
    // not part of the cache identity.
    val plain = OutputPort(PortIdentity(0))
    val decorated = OutputPort(
      PortIdentity(0),
      blocking = true,
      mode = OutputPort.OutputMode.SET_DELTA,
      reuseStorage = true
    )
    keyOf(planWith(plain), "a").hash shouldEqual keyOf(planWith(decorated), "a").hash
  }

  "CacheKeyUtil.isSameComputation" should "treat two keys with the same hash and JSON as a match" in {
    val plan = linearPlan()
    CacheKeyUtil.isSameComputation(keyOf(plan, "b"), keyOf(plan, "b")) shouldBe true
  }

  it should "reject a hash collision by comparing the full JSON" in {
    // Two different computations that hash to the same value (fabricated, since a
    // real SHA-256 collision is infeasible to construct): the JSON differs, so the
    // match is rejected and a cached result is never reused for the wrong port.
    CacheKeyUtil.isSameComputation(
      StorageCacheKey("upstream-A", "same-hash"),
      StorageCacheKey("upstream-B", "same-hash")
    ) shouldBe false
  }

  it should "reject keys with different hashes" in {
    CacheKeyUtil.isSameComputation(
      StorageCacheKey("upstream-A", "hash-1"),
      StorageCacheKey("upstream-A", "hash-2")
    ) shouldBe false
  }
}
