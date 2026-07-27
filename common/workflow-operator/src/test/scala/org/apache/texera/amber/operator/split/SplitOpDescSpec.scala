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

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SplitOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private val schema: Schema =
    Schema().add(new Attribute("v", AttributeType.INTEGER))

  // ---------------------------------------------------------------------------
  // operatorInfo
  // ---------------------------------------------------------------------------

  "SplitOpDesc.operatorInfo" should
    "advertise the Split user-friendly name and Utility group" in {
    val info = (new SplitOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Split"
    info.operatorGroupName shouldBe OperatorGroupConstants.UTILITY_GROUP
    info.operatorDescription.toLowerCase should include("split")
  }

  it should "expose one input port and two output ports (PortIdentity 0 and 1)" in {
    val info = (new SplitOpDesc).operatorInfo
    info.inputPorts should have length 1
    info.outputPorts should have length 2
    info.outputPorts.map(_.id) shouldBe List(PortIdentity(), PortIdentity(1))
  }

  // ---------------------------------------------------------------------------
  // Field defaults
  // ---------------------------------------------------------------------------

  "SplitOpDesc fields" should "default k to 80, random to true, seed to 1" in {
    val d = new SplitOpDesc
    d.k shouldBe 80
    d.random shouldBe true
    d.seed shouldBe 1
  }

  // ---------------------------------------------------------------------------
  // getPhysicalOp — wiring to SplitOpExec + non-parallelizable
  // ---------------------------------------------------------------------------

  "SplitOpDesc.getPhysicalOp" should
    "wire the SplitOpExec class name into the OpExecInitInfo" in {
    val physical = (new SplitOpDesc).getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.split.SplitOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
  }

  it should "carry a serialized descriptor JSON in the OpExecInitInfo payload" in {
    // The descriptor's `k` / `random` / `seed` must be reachable at the
    // executor via the serialized payload — pin that the JSON includes
    // the canonical wire keys.
    val physical = (new SplitOpDesc).getPhysicalOp(workflowId, executionId)
    val payload = physical.opExecInitInfo match {
      case OpExecWithClassName(_, p) => p
      case other                     => fail(s"expected OpExecWithClassName, got $other")
    }
    payload should include("\"k\"")
    payload should include("\"random\"")
    payload should include("\"seed\"")
  }

  it should "be non-parallelizable (single worker)" in {
    // Split's deterministic-seed contract relies on a single
    // worker — `withParallelizable(false)` is the wiring under test.
    val physical = (new SplitOpDesc).getPhysicalOp(workflowId, executionId)
    physical.parallelizable shouldBe false
  }

  // ---------------------------------------------------------------------------
  // Schema propagation
  // ---------------------------------------------------------------------------

  "SplitOpDesc schema propagation" should
    "propagate the single input schema to every output port" in {
    val physical = (new SplitOpDesc).getPhysicalOp(workflowId, executionId)
    val out = physical.propagateSchema.func(Map(PortIdentity() -> schema))
    val descInfo = (new SplitOpDesc).operatorInfo
    out.keySet shouldBe descInfo.outputPorts.map(_.id).toSet
    out.values.toSet shouldBe Set(schema)
  }

  it should
    "throw IllegalArgumentException when the input map does not have exactly one entry" in {
    val physical = (new SplitOpDesc).getPhysicalOp(workflowId, executionId)
    intercept[IllegalArgumentException] {
      physical.propagateSchema.func(Map.empty)
    }
    intercept[IllegalArgumentException] {
      physical.propagateSchema.func(
        Map(PortIdentity() -> schema, PortIdentity(1) -> schema)
      )
    }
  }

  // ---------------------------------------------------------------------------
  // generateStandaloneCode
  // ---------------------------------------------------------------------------

  "SplitOpDesc.generateStandaloneCode" should
    "draw an unseeded mask and partition the input across both outputs" in {
    (new SplitOpDesc).generateStandaloneCode() shouldBe
      """import numpy as np
        |_split_mask = np.random.RandomState(None).rand(len(in1df)) < 0.8
        |out1df = in1df[_split_mask].reset_index(drop=True)
        |out2df = in1df[~_split_mask].reset_index(drop=True)""".stripMargin
  }

  it should "pass the configured seed to RandomState when auto-generate is off" in {
    val d = new SplitOpDesc
    d.random = false
    d.seed = 42
    d.generateStandaloneCode() should include("np.random.RandomState(42).rand(len(in1df))")
  }

  // k is a percentage on the descriptor but a fraction in the generated code;
  // an integer division here would collapse every split to 0.
  it should "convert the split percentage to a fraction" in {
    Seq(0 -> "0.0", 30 -> "0.3", 80 -> "0.8", 100 -> "1.0").foreach {
      case (k, frac) =>
        val d = new SplitOpDesc
        d.k = k
        d.generateStandaloneCode() should include(s"< $frac")
    }
  }

  // ---------------------------------------------------------------------------
  // Independent instances
  // ---------------------------------------------------------------------------

  "SplitOpDesc" should
    "assign a fresh operatorIdentifier per instance (UUID-based id is not shared)" in {
    val a = new SplitOpDesc
    val b = new SplitOpDesc
    a.operatorIdentifier should not equal b.operatorIdentifier
  }
}
