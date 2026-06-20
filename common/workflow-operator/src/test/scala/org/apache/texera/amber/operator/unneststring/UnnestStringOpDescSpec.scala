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

package org.apache.texera.amber.operator.unneststring

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UnnestStringOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def newDesc(delim: String, attr: String, result: String): UnnestStringOpDesc = {
    val d = new UnnestStringOpDesc
    d.delimiter = delim
    d.attribute = attr
    d.resultAttribute = result
    d
  }

  "UnnestStringOpDesc.operatorInfo" should "advertise the name and Utility group" in {
    val info = (new UnnestStringOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Unnest String"
    info.operatorGroupName shouldBe OperatorGroupConstants.UTILITY_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "UnnestStringOpDesc.getPhysicalOp" should "wire UnnestStringOpExec and carry port identities" in {
    val op = newDesc(",", "tags", "tag")
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.unneststring.UnnestStringOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "UnnestStringOpDesc schema propagation" should
    "append a STRING column named by resultAttribute" in {
    val op = newDesc(",", "tags", "tag")
    val physical = op.getPhysicalOp(workflowId, executionId)
    val input = Schema().add(new Attribute("tags", AttributeType.STRING))
    val out = physical.propagateSchema.func(Map(op.operatorInfo.inputPorts.head.id -> input))
    out shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> input.add(new Attribute("tag", AttributeType.STRING))
    )
  }

  it should "throw when resultAttribute is blank" in {
    val op = newDesc(",", "tags", "")
    val physical = op.getPhysicalOp(workflowId, executionId)
    val input = Schema().add(new Attribute("tags", AttributeType.STRING))
    intercept[RuntimeException] {
      physical.propagateSchema.func(Map(op.operatorInfo.inputPorts.head.id -> input))
    }
  }

  "UnnestStringOpDesc" should "round-trip its fields through the polymorphic base" in {
    val restored =
      objectMapper.readValue(
        objectMapper.writeValueAsString(newDesc(";", "csv", "item")),
        classOf[LogicalOp]
      )
    restored shouldBe a[UnnestStringOpDesc]
    val u = restored.asInstanceOf[UnnestStringOpDesc]
    u.delimiter shouldBe ";"
    u.attribute shouldBe "csv"
    u.resultAttribute shouldBe "item"
  }
}
