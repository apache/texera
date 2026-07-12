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

package org.apache.texera.amber.operator

import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort}
import org.apache.texera.amber.operator.distinct.DistinctOpDesc
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.scalatest.flatspec.AnyFlatSpec

class LogicalOpSpec extends AnyFlatSpec {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  // a minimal subtype that leaves getPhysicalOp unoverridden to hit the base `???` default
  private class BareLogicalOp extends LogicalOp {
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "Bare",
        "",
        OperatorGroupConstants.UTILITY_GROUP,
        inputPorts = List(InputPort()),
        outputPorts = List(OutputPort())
      )
  }

  "LogicalOp.getPhysicalOp" should "throw NotImplementedError when a subtype does not override it" in {
    assertThrows[NotImplementedError](new BareLogicalOp().getPhysicalOp(workflowId, executionId))
  }

  "LogicalOp.hashCode/equals" should "be reflection-based over the operator's fields" in {
    val a = new DistinctOpDesc
    a.setOperatorId("fixed-id")
    assert(a.hashCode == a.hashCode) // deterministic
    val b = new DistinctOpDesc
    b.setOperatorId("fixed-id")
    b.operatorVersion = a.operatorVersion
    assert(a == b && a.hashCode == b.hashCode)
  }

  "LogicalOp.toString" should "render the class name and operator id via reflection" in {
    val op = new DistinctOpDesc
    op.setOperatorId("distinct-1")
    val rendered = op.toString
    assert(rendered.contains("DistinctOpDesc"))
    assert(rendered.contains("distinct-1"))
  }

  "LogicalOp.runtimeReconfiguration" should "throw UnsupportedOperationException by default" in {
    val op = new DistinctOpDesc
    val ex = intercept[UnsupportedOperationException] {
      op.runtimeReconfiguration(workflowId, executionId, op, op)
    }
    assert(ex.getMessage == "operator DistinctOpDesc does not support reconfiguration")
  }
}
