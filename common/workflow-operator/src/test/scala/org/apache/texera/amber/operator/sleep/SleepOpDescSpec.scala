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

package org.apache.texera.amber.operator.sleep

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SleepOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "SleepOpDesc.operatorInfo" should "advertise the user-friendly name and Control group" in {
    val info = (new SleepOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Sleep"
    info.operatorGroupName shouldBe OperatorGroupConstants.CONTROL_GROUP
    info.operatorDescription should include("Sleep")
  }

  it should "expose exactly one input port and one output port" in {
    val info = (new SleepOpDesc).operatorInfo
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "SleepOpDesc.getPhysicalOp" should "produce a non-parallelizable PhysicalOp pinned to a single worker" in {
    // Sleep is non-parallelizable on purpose: tuples must traverse the
    // sleep path serially so the delay is observable as a back-pressure
    // signal upstream. The descriptor pins both flags explicitly.
    val op = new SleepOpDesc
    op.sleepTime = 5
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.parallelizable shouldBe false
    physical.suggestedWorkerNum shouldBe Some(1)
  }

  it should "wire the SleepOpExec class name into the OpExecInitInfo" in {
    // The descriptor's getPhysicalOp encodes a fully-qualified Exec class
    // name; pin it so a rename of SleepOpExec breaks this spec deliberately.
    // Pattern-match on OpExecWithClassName instead of substring-matching the
    // toString output, which is brittle to scalapb formatting changes.
    val op = new SleepOpDesc
    op.sleepTime = 1
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.sleep.SleepOpExec"
        descString should not be empty
      case other =>
        fail(s"expected OpExecWithClassName, got $other")
    }
  }

  it should "carry forward the operatorInfo input/output ports onto the PhysicalOp" in {
    val op = new SleepOpDesc
    op.sleepTime = 1
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.inputPorts.size shouldBe op.operatorInfo.inputPorts.size
    physical.outputPorts.size shouldBe op.operatorInfo.outputPorts.size
  }
}
