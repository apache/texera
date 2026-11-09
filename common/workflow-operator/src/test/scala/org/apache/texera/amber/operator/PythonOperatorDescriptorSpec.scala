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

import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  InputPort,
  OutputPort,
  PortIdentity,
  PreferCoordinator
}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PythonOperatorDescriptorSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private class OneToOnePyOp extends PythonOperatorDescriptor {
    override def generatePythonCode(): String = "print('hi')"
    override def getOutputSchemas(in: Map[PortIdentity, Schema]): Map[PortIdentity, Schema] =
      Map.empty
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "t",
        "d",
        OperatorGroupConstants.PYTHON_GROUP,
        List(InputPort()),
        List(OutputPort())
      )
  }

  private class SourcePyOp extends PythonOperatorDescriptor {
    override def asSource(): Boolean = true
    override def parallelizable(): Boolean = true
    override def generatePythonCode(): String = "yield None"
    override def getOutputSchemas(in: Map[PortIdentity, Schema]): Map[PortIdentity, Schema] =
      Map.empty
    override def operatorInfo: OperatorInfo =
      OperatorInfo("s", "d", OperatorGroupConstants.PYTHON_GROUP, List(), List(OutputPort()))
  }

  private class ThrowingPyOp extends PythonOperatorDescriptor {
    override def generatePythonCode(): String = throw new RuntimeException("boom")
    override def getOutputSchemas(in: Map[PortIdentity, Schema]): Map[PortIdentity, Schema] =
      Map.empty
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "x",
        "d",
        OperatorGroupConstants.PYTHON_GROUP,
        List(InputPort()),
        List(OutputPort())
      )
  }

  private def code(op: PythonOperatorDescriptor): String =
    op.getPhysicalOp(workflowId, executionId).opExecInitInfo match {
      case OpExecWithCode(c, _) => c
      case other                => fail(s"expected OpExecWithCode, got $other")
    }

  "PythonOperatorDescriptor.getPhysicalOp" should
    "embed the generation exception in the code instead of throwing" in {
    code(new ThrowingPyOp) shouldBe "#EXCEPTION DURING CODE GENERATION: boom"
  }

  it should "build a source PhysicalOp when asSource is true" in {
    val physical = (new SourcePyOp).getPhysicalOp(workflowId, executionId)
    physical.locationPreference shouldBe Some(PreferCoordinator)
    physical.opExecInitInfo match {
      case OpExecWithCode(c, language) =>
        c shouldBe "yield None"
        language shouldBe "python"
      case other => fail(s"expected OpExecWithCode, got $other")
    }
    physical.isSourceOperator shouldBe true
  }

  it should "build a one-to-one PhysicalOp when asSource is false" in {
    val physical = (new OneToOnePyOp).getPhysicalOp(workflowId, executionId)
    physical.locationPreference shouldBe None
    physical.opExecInitInfo match {
      case OpExecWithCode(c, _) => c shouldBe "print('hi')"
      case other                => fail(s"expected OpExecWithCode, got $other")
    }
  }
}
