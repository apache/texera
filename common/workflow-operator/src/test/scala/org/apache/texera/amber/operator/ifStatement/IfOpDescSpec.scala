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

package org.apache.texera.amber.operator.ifStatement

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IfOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "IfOpDesc.operatorInfo" should
    "advertise two inputs (Condition + data) and two outputs (False/True) in the Control group" in {
    val info = (new IfOpDesc).operatorInfo
    info.userFriendlyName shouldBe "If"
    info.operatorGroupName shouldBe OperatorGroupConstants.CONTROL_GROUP
    info.inputPorts should have length 2
    info.inputPorts.head.id shouldBe PortIdentity()
    info.inputPorts.head.displayName shouldBe "Condition"
    info.inputPorts.last.id shouldBe PortIdentity(1)
    info.outputPorts.map(_.id) shouldBe List(PortIdentity(), PortIdentity(1))
    info.outputPorts.head.displayName shouldBe "False"
    info.outputPorts.last.displayName shouldBe "True"
  }

  "IfOpDesc.conditionName" should "default to null" in {
    (new IfOpDesc).conditionName shouldBe null
  }

  "IfOpDesc.getPhysicalOp" should
    "wire IfOpExec, be non-parallelizable, and carry the port identities" in {
    val op = new IfOpDesc
    op.conditionName = "ready"
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.parallelizable shouldBe false
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.ifStatement.IfOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "IfOpDesc schema propagation" should
    "route the data input's schema (inputPorts.last) to BOTH outputs, dropping the condition schema" in {
    val physical = (new IfOpDesc).getPhysicalOp(workflowId, executionId)
    val condSchema = Schema().add(new Attribute("cond", AttributeType.BOOLEAN))
    val dataSchema = Schema().add(new Attribute("payload", AttributeType.STRING))
    val out = physical.propagateSchema.func(
      Map(PortIdentity() -> condSchema, PortIdentity(1) -> dataSchema)
    )
    out shouldBe Map(PortIdentity() -> dataSchema, PortIdentity(1) -> dataSchema)
  }
}
