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

import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import java.lang.reflect.Modifier
import org.scalatest.flatspec.AnyFlatSpec

class FilterOpDescSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Test-only concrete subclass — same shape as the MapOpDesc stub; the
  // two abstract bases share the same `runtimeReconfiguration` contract.
  // ---------------------------------------------------------------------------

  private class StubFilterDesc extends FilterOpDesc {
    var calls: List[(WorkflowIdentity, ExecutionIdentity)] = Nil
    override def getPhysicalOp(
        workflowId: WorkflowIdentity,
        executionId: ExecutionIdentity
    ): PhysicalOp = {
      calls = calls :+ ((workflowId, executionId))
      null.asInstanceOf[PhysicalOp]
    }
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "StubFilter",
        "stub filter",
        OperatorGroupConstants.UTILITY_GROUP,
        inputPorts = List.empty,
        outputPorts = List.empty
      )
  }

  private val workflowId = WorkflowIdentity(7L)
  private val executionId = ExecutionIdentity(11L)

  // ---------------------------------------------------------------------------
  // Abstract-class shape
  // ---------------------------------------------------------------------------

  "FilterOpDesc" should "be declared abstract (cannot be instantiated directly)" in {
    assert(Modifier.isAbstract(classOf[FilterOpDesc].getModifiers))
  }

  // ---------------------------------------------------------------------------
  // Inheritance — FilterOpDesc is a LogicalOp
  // ---------------------------------------------------------------------------

  it should "extend LogicalOp (compile-time enforced)" in {
    val s: LogicalOp = new StubFilterDesc
    assert(s != null)
  }

  // ---------------------------------------------------------------------------
  // runtimeReconfiguration — delegates to newOpDesc and ignores oldOpDesc
  // ---------------------------------------------------------------------------

  "FilterOpDesc.runtimeReconfiguration" should
    "delegate to newOpDesc.getPhysicalOp with the supplied workflow/execution ids" in {
    val oldDesc = new StubFilterDesc
    val newDesc = new StubFilterDesc
    val result = oldDesc.runtimeReconfiguration(workflowId, executionId, oldDesc, newDesc)
    assert(result.isSuccess)
    assert(newDesc.calls == List((workflowId, executionId)))
  }

  it should "not call oldOpDesc.getPhysicalOp" in {
    val oldDesc = new StubFilterDesc
    val newDesc = new StubFilterDesc
    oldDesc.runtimeReconfiguration(workflowId, executionId, oldDesc, newDesc)
    assert(oldDesc.calls == Nil)
  }

  it should "return None for the StateTransferFunc slot" in {
    val oldDesc = new StubFilterDesc
    val newDesc = new StubFilterDesc
    val result = oldDesc.runtimeReconfiguration(workflowId, executionId, oldDesc, newDesc)
    val (_, transferOpt) = result.get
    assert(transferOpt.isEmpty)
  }

  it should "propagate exceptions from newOpDesc.getPhysicalOp (not catch them)" in {
    val oldDesc = new StubFilterDesc
    val throwingDesc = new StubFilterDesc {
      override def getPhysicalOp(
          workflowId: WorkflowIdentity,
          executionId: ExecutionIdentity
      ): PhysicalOp = throw new RuntimeException("sentinel:newDesc")
    }
    val ex = intercept[RuntimeException] {
      oldDesc.runtimeReconfiguration(workflowId, executionId, oldDesc, throwingDesc)
    }
    assert(ex.getMessage == "sentinel:newDesc")
  }
}
