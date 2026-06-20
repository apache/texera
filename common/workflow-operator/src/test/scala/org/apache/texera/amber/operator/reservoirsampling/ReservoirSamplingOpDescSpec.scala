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

package org.apache.texera.amber.operator.reservoirsampling

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReservoirSamplingOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private val WireKey = "number of item sampled in reservoir sampling"

  "ReservoirSamplingOpDesc.operatorInfo" should
    "advertise the name, Utility group, and (intentionally) NOT support reconfiguration" in {
    val info = (new ReservoirSamplingOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Reservoir Sampling"
    info.operatorDescription shouldBe "Reservoir Sampling with k items being kept randomly"
    info.operatorGroupName shouldBe OperatorGroupConstants.UTILITY_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    // ReservoirSampling does not opt into reconfiguration (unlike RandomKSampling),
    // so it inherits the OperatorInfo default of false.
    info.supportReconfiguration shouldBe false
  }

  "ReservoirSamplingOpDesc" should "serialize k under its wire-key and round-trip it" in {
    val d = new ReservoirSamplingOpDesc
    d.k = 100
    val json = objectMapper.writeValueAsString(d)
    val tree = objectMapper.readTree(json)
    tree.has(WireKey) shouldBe true
    tree.get(WireKey).asInt shouldBe 100
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[ReservoirSamplingOpDesc]
    restored.asInstanceOf[ReservoirSamplingOpDesc].k shouldBe 100
  }

  "ReservoirSamplingOpDesc.getPhysicalOp" should
    "wire the ReservoirSamplingOpExec class name and carry ports" in {
    val d = new ReservoirSamplingOpDesc
    d.k = 10
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.reservoirsampling.ReservoirSamplingOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe d.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }
}
