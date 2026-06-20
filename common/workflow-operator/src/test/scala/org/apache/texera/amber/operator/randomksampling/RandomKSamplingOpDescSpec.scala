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

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RandomKSamplingOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  // The percentage field uses a spaced @JsonProperty wire-key.
  private val WireKey = "random k sample percentage"

  "RandomKSamplingOpDesc.operatorInfo" should
    "advertise the name, Utility group, and reconfiguration support" in {
    val info = (new RandomKSamplingOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Random K Sampling"
    info.operatorDescription shouldBe "random sampling with given percentage"
    info.operatorGroupName shouldBe OperatorGroupConstants.UTILITY_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "RandomKSamplingOpDesc" should
    "serialize percentage under its spaced wire-key and round-trip it" in {
    val d = new RandomKSamplingOpDesc
    d.percentage = 25
    val json = objectMapper.writeValueAsString(d)
    val tree = objectMapper.readTree(json)
    tree.has(WireKey) shouldBe true
    tree.get(WireKey).asInt shouldBe 25
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[RandomKSamplingOpDesc]
    restored.asInstanceOf[RandomKSamplingOpDesc].percentage shouldBe 25
  }

  "RandomKSamplingOpDesc.getPhysicalOp" should
    "wire the RandomKSamplingOpExec class name and carry ports" in {
    val d = new RandomKSamplingOpDesc
    d.percentage = 50
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.randomksampling.RandomKSamplingOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe d.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }
}
