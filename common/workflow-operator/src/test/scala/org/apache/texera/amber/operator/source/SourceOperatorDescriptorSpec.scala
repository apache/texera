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

package org.apache.texera.amber.operator.source

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.scalatest.flatspec.AnyFlatSpec

class SourceOperatorDescriptorSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Test-only concrete subclass — exposes the abstract `sourceSchema()`
  // and the inherited LogicalOp abstract members so the contract is
  // observable end-to-end.
  // ---------------------------------------------------------------------------

  private val testSchema: Schema =
    Schema().add(new Attribute("col", AttributeType.STRING))

  private class StubSource extends SourceOperatorDescriptor {
    override def sourceSchema(): Schema = testSchema
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "Stub",
        "stub source",
        OperatorGroupConstants.INPUT_GROUP,
        inputPorts = List.empty,
        outputPorts = List.empty
      )
    override def getPhysicalOp(
        workflowId: WorkflowIdentity,
        executionId: ExecutionIdentity
    ): PhysicalOp =
      throw new NotImplementedError(
        "getPhysicalOp is not needed for the SourceOperatorDescriptor contract test"
      )
  }

  // ---------------------------------------------------------------------------
  // sourceSchema — abstract member is observable
  // ---------------------------------------------------------------------------

  "SourceOperatorDescriptor (concrete subclass)" should
    "expose the `sourceSchema()` value supplied by the impl" in {
    val s = new StubSource
    assert(s.sourceSchema() == testSchema)
  }

  // ---------------------------------------------------------------------------
  // Inheritance — SourceOperatorDescriptor is a LogicalOp
  // ---------------------------------------------------------------------------

  it should "be a LogicalOp (compile-time enforced)" in {
    val s: LogicalOp = new StubSource
    assert(s != null)
  }

  it should "match the LogicalOp type-pattern" in {
    val any: AnyRef = new StubSource
    val matched = any match {
      case _: LogicalOp => true
      case _            => false
    }
    assert(matched)
  }

  it should "match the SourceOperatorDescriptor type-pattern" in {
    val any: AnyRef = new StubSource
    val matched = any match {
      case _: SourceOperatorDescriptor => true
      case _                           => false
    }
    assert(matched)
  }
}
