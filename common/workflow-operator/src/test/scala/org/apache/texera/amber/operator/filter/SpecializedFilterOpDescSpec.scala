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

package org.apache.texera.amber.operator.filter

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SpecializedFilterOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "SpecializedFilterOpDesc.operatorInfo" should
    "advertise the name, Cleaning group, and reconfiguration support" in {
    val info = (new SpecializedFilterOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Filter"
    info.operatorGroupName shouldBe OperatorGroupConstants.CLEANING_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "SpecializedFilterOpDesc.predicates" should "default to an empty list" in {
    (new SpecializedFilterOpDesc).predicates shouldBe empty
  }

  "SpecializedFilterOpDesc.getPhysicalOp" should
    "wire SpecializedFilterOpExec and carry port identities" in {
    val op = new SpecializedFilterOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.filter.SpecializedFilterOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "SpecializedFilterOpDesc" should
    "round-trip (default empty predicates) through the polymorphic base" in {
    val restored =
      objectMapper.readValue(
        objectMapper.writeValueAsString(new SpecializedFilterOpDesc),
        classOf[LogicalOp]
      )
    restored shouldBe a[SpecializedFilterOpDesc]
    restored.asInstanceOf[SpecializedFilterOpDesc].predicates shouldBe empty
  }
}
