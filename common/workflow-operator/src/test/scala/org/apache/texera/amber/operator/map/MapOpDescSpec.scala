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

package org.apache.texera.amber.operator.map

import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import java.lang.reflect.Modifier
import org.scalatest.flatspec.AnyFlatSpec

class MapOpDescSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Test-only concrete subclass — records every call to `getPhysicalOp`
  // and returns a stable sentinel so the spec can verify the delegation.
  //
  // Casting `null` to PhysicalOp is acceptable here: the production
  // `runtimeReconfiguration` just wraps the return value in
  // `Success((_, None))` without inspecting it.
  // ---------------------------------------------------------------------------

  private class StubMapDesc extends MapOpDesc {
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
        "StubMap",
        "stub map",
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

  "MapOpDesc" should "be declared abstract (cannot be instantiated directly)" in {
    assert(Modifier.isAbstract(classOf[MapOpDesc].getModifiers))
  }

  // ---------------------------------------------------------------------------
  // Inheritance — MapOpDesc is a LogicalOp
  // ---------------------------------------------------------------------------

  it should "extend LogicalOp (compile-time enforced)" in {
    val s: LogicalOp = new StubMapDesc
    assert(s != null)
  }

  // ---------------------------------------------------------------------------
  // runtimeReconfiguration — delegates to newOpDesc and ignores oldOpDesc
  // ---------------------------------------------------------------------------

  "MapOpDesc.runtimeReconfiguration" should
    "delegate to newOpDesc.getPhysicalOp with the supplied workflow/execution ids" in {
    val oldDesc = new StubMapDesc
    val newDesc = new StubMapDesc
    val result = oldDesc.runtimeReconfiguration(workflowId, executionId, oldDesc, newDesc)
    assert(result.isSuccess)
    assert(newDesc.calls == List((workflowId, executionId)))
  }

  it should "not call oldOpDesc.getPhysicalOp" in {
    val oldDesc = new StubMapDesc
    val newDesc = new StubMapDesc
    oldDesc.runtimeReconfiguration(workflowId, executionId, oldDesc, newDesc)
    assert(oldDesc.calls == Nil)
  }

  it should "return None for the StateTransferFunc slot" in {
    val oldDesc = new StubMapDesc
    val newDesc = new StubMapDesc
    val result = oldDesc.runtimeReconfiguration(workflowId, executionId, oldDesc, newDesc)
    val (_, transferOpt) = result.get
    assert(transferOpt.isEmpty)
  }

  it should "propagate exceptions from newOpDesc.getPhysicalOp (not catch them)" in {
    val oldDesc = new StubMapDesc
    val throwingDesc = new StubMapDesc {
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
