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

package org.apache.texera.amber.operator.dummy

import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity, UnknownPartition}
import org.apache.texera.amber.operator.PortDescription
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DummyOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "DummyOpDesc.operatorInfo" should
    "advertise the Utility group and enable all dynamic / reconfiguration / customization flags" in {
    val info = (new DummyOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Dummy"
    info.operatorDescription shouldBe "A dummy operator used as a placeholder."
    info.operatorGroupName shouldBe OperatorGroupConstants.UTILITY_GROUP
    info.dynamicInputPorts shouldBe true
    info.dynamicOutputPorts shouldBe true
    info.supportReconfiguration shouldBe true
    info.allowPortCustomization shouldBe true
  }

  "DummyOpDesc" should
    "derive a single default input/output port when the port lists are unset (null)" in {
    val info = (new DummyOpDesc).operatorInfo
    info.inputPorts shouldBe List(InputPort())
    info.outputPorts shouldBe List(OutputPort())
  }

  it should "derive ports from an explicit PortDescriptor list, indexed by position" in {
    val d = new DummyOpDesc
    d.inputPorts = List(
      PortDescription(
        "p0",
        "first",
        disallowMultiInputs = false,
        isDynamicPort = false,
        UnknownPartition()
      )
    )
    val ports = d.operatorInfo.inputPorts
    ports should have length 1
    ports.head.id shouldBe PortIdentity(0)
    ports.head.displayName shouldBe "first"
  }

  "DummyOpDesc.dummyOperator" should "default to the empty string" in {
    (new DummyOpDesc).dummyOperator shouldBe ""
  }

  "DummyOpDesc.getPhysicalOp" should
    "be the unimplemented LogicalOp stub (throws NotImplementedError)" in {
    intercept[NotImplementedError] {
      (new DummyOpDesc).getPhysicalOp(workflowId, executionId)
    }
  }
}
