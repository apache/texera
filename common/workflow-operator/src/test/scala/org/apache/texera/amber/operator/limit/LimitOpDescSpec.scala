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

package org.apache.texera.amber.operator.limit

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Success

class LimitOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  // LogicalOp carries @JsonTypeInfo(property = "operatorType"); deserialize via the
  // base type with the registered discriminator name "Limit".
  private def limitDesc(n: Int): LimitOpDesc =
    objectMapper
      .readValue(s"""{"operatorType":"Limit","limit":$n}""", classOf[LogicalOp])
      .asInstanceOf[LimitOpDesc]

  "LimitOpDesc.operatorInfo" should
    "advertise the name, Cleaning group, and reconfiguration support" in {
    val info = (new LimitOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Limit"
    info.operatorDescription shouldBe "Limit the number of output rows"
    info.operatorGroupName shouldBe OperatorGroupConstants.CLEANING_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "LimitOpDesc" should "deserialize the limit field through the polymorphic base" in {
    limitDesc(42).limit shouldBe 42
  }

  "LimitOpDesc.getPhysicalOp" should "be non-parallelizable and wire LimitOpExec" in {
    val physical = limitDesc(10).getPhysicalOp(workflowId, executionId)
    physical.parallelizable shouldBe false
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.limit.LimitOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
  }

  it should "carry forward the operatorInfo input/output port identities" in {
    val op = limitDesc(10)
    val physical = op.getPhysicalOp(workflowId, executionId)
    // Pin the actual port identities (not just counts).
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "LimitOpDesc.runtimeReconfiguration" should
    "return Success with a state-transfer func that copies the running row count" in {
    val desc = limitDesc(5)
    val result = desc.runtimeReconfiguration(workflowId, executionId, desc, desc)
    result shouldBe a[Success[_]]
    val (_, transferOpt) = result.get
    transferOpt should not be empty

    // Exercise the state-transfer func: a freshly-created exec starts at count 0;
    // the func must copy the old exec's count into the new one.
    val descJson = """{"operatorType":"Limit","limit":5}"""
    val oldExec = new LimitOpExec(descJson)
    oldExec.count = 3
    val newExec = new LimitOpExec(descJson)
    newExec.count shouldBe 0
    val transfer = transferOpt.get
    transfer(oldExec, newExec)
    newExec.count shouldBe 3
  }
}
