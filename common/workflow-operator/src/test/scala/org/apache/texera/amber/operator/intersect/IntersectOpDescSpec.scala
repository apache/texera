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

package org.apache.texera.amber.operator.intersect

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{HashPartition, SinglePartition, UnknownPartition}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IntersectOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "IntersectOpDesc.operatorInfo" should "advertise the user-friendly name and Set group" in {
    val info = (new IntersectOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Intersect"
    info.operatorGroupName shouldBe OperatorGroupConstants.SET_GROUP
    info.operatorDescription should include("intersect")
  }

  it should "expose two input ports (PortIdentity 0 and 1) and one blocking output" in {
    val info = (new IntersectOpDesc).operatorInfo
    info.inputPorts should have length 2
    info.inputPorts.map(_.id.id) shouldBe List(0, 1)
    info.outputPorts should have length 1
    info.outputPorts.head.blocking shouldBe true
  }

  "IntersectOpDesc.getPhysicalOp" should "require HashPartition on both input ports" in {
    val op = new IntersectOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.partitionRequirement shouldBe List(
      Option(HashPartition()),
      Option(HashPartition())
    )
  }

  it should "always derive HashPartition for the output regardless of input partitions" in {
    // The Intersect set semantics require both inputs to be hash-aligned, so
    // the derived output partition must remain hash even when the upstream
    // inputs report differing partition kinds.
    val op = new IntersectOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.derivePartition(List(SinglePartition(), UnknownPartition())) shouldBe HashPartition()
    physical.derivePartition(
      List(HashPartition(List("a")), HashPartition(List("b")))
    ) shouldBe HashPartition()
  }

  it should "wire the IntersectOpExec class name into the OpExecInitInfo" in {
    // Pattern-match on OpExecWithClassName instead of substring-matching the
    // toString output, which is brittle to scalapb formatting changes.
    val op = new IntersectOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.intersect.IntersectOpExec"
      case other =>
        fail(s"expected OpExecWithClassName, got $other")
    }
  }
}
